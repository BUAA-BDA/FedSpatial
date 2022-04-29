package com.hufudb.onedb.core.sql.expression;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.data.TypeConverter;
import com.hufudb.onedb.rpc.OneDBCommon.ExpressionProto;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

/*
 * tree node of expression tree
 */
public class OneDBOperator implements OneDBExpression {
  OneDBOpType opType;
  FieldType outType;
  List<OneDBExpression> inputs;
  FuncType funcType;
  Level level;

  OneDBOperator(OneDBOpType opType, FieldType outType, List<OneDBExpression> inputs,
      FuncType funcType, Level level) {
    this.opType = opType;
    this.outType = outType;
    this.inputs = inputs;
    this.funcType = funcType;
    this.level = level;
  }

  OneDBOperator(OneDBOpType opType, FieldType outType, List<OneDBExpression> inputs,
      FuncType funcType) {
    this(opType, outType, inputs, funcType, Level.findDominator(inputs));
  }

  public static OneDBExpression fromRexNode(RexNode node, List<OneDBExpression> ins) {
    return new OperatorBuilder(ImmutableList.of(node), ins).build().get(0);
  }

  public static List<OneDBExpression> fromRexNodes(List<RexNode> nodes, List<OneDBExpression> ins) {
    return nodes.stream().map(node -> {
      if (node instanceof RexInputRef) {
        int i = ((RexInputRef) node).getIndex();
        OneDBExpression exp = ins.get(i);
        if (exp instanceof RexInputRef) {
          return new OneDBReference(exp.getOutType(), exp.getLevel(), i);
        } else {
          return exp;
        }
      } else {
        return OneDBOperator.fromRexNode(node, ins);
      }
    }).collect(Collectors.toList());
  }

  public static List<OneDBExpression> fromRexNodes(RexProgram program, List<OneDBExpression> ins) {
    return new OperatorBuilder(program.getExprList(), program.getProjectList(), ins).build();
  }

  public static OneDBOperator create(OneDBOpType opType, FieldType outType,
      List<OneDBExpression> inputs, FuncType funcType) {
    return new OneDBOperator(opType, outType, inputs, funcType);
  }

  /*
   * functions to build operator tree
   */

  public ExpressionProto toProto() {
    ExpressionProto.Builder builder = ExpressionProto.newBuilder();
    return builder.setOpType(opType.ordinal())
        .setOutType(outType.ordinal())
        .setLevel(level.getId())
        .addAllIn(inputs.stream().map(e -> e.toProto()).collect(Collectors.toList()))
        .setFunc(funcType.ordinal()).build();
  }

  public List<OneDBExpression> getInputs() {
    return inputs;
  }

  public FuncType getFuncType() {
    return funcType;
  }

  @Override
  public FieldType getOutType() {
    return outType;
  }

  @Override
  public OneDBOpType getOpType() {
    return opType;
  }

  @Override
  public Level getLevel() {
    return level;
  }

  public enum FuncType {
    NONE, ABS;

    public static FuncType of(int id) {
      return FuncType.values()[id];
    }
  }

  private static class OperatorBuilder {
    List<? extends RexNode> outputNodes;
    List<RexNode> localNodes;
    List<OneDBExpression> inputExps;

    OperatorBuilder(List<RexNode> nodes, List<OneDBExpression> inputs) {
      this.outputNodes = nodes;
      this.localNodes = nodes;
      this.inputExps = inputs;
    }

    OperatorBuilder(List<RexNode> localNodes, List<? extends RexNode> outputNodes,
        List<OneDBExpression> inputs) {
      this.outputNodes = outputNodes;
      this.localNodes = localNodes;
      this.inputExps = inputs;
    }

    List<OneDBExpression> build() {
      return outputNodes.stream().map(node -> buildOp(node)).collect(Collectors.toList());
    }

    OneDBExpression buildOp(RexNode node) {
      switch (node.getKind()) {
        // leaf node
        case LITERAL:
        case INPUT_REF:
          return leaf(node);
        // binary
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
        case EQUALS:
        case NOT_EQUALS:
        case PLUS:
        case MINUS:
        case TIMES:
        case DIVIDE:
        case MOD:
        case AND:
        case OR:
          return binary((RexCall) node);
        // unary
        case AS:
        case NOT:
        case PLUS_PREFIX:
        case MINUS_PREFIX:
        case IS_NULL:
        case IS_NOT_NULL:
          return unary((RexCall) node);
        // case
        case CASE:
          return caseCall((RexCall) node);
        case SEARCH:
          return searchCall((RexCall) node);
        // local_ref
        case LOCAL_REF:
          return localRef((RexLocalRef) node);
        // udf
        case OTHER_FUNCTION:
          return scalarFunc((RexCall) node);
        default:
          throw new RuntimeException(String.format("not support %s", node));
      }
    }

    /*
     * only accept literal and input reference node
     */
    OneDBExpression leaf(RexNode node) {
      switch (node.getKind()) {
        case LITERAL:
          return OneDBLiteral.fromLiteral((RexLiteral) node);
        case INPUT_REF:
          return inputExps.get(((RexInputRef) node).getIndex());
        default:
          throw new RuntimeException("can't translate " + node);
      }
    }

    /*
     * add binary operator
     */
    OneDBExpression binary(RexCall call) {
      OneDBOpType op;
      switch (call.getKind()) {
        case GREATER_THAN:
          op = OneDBOpType.GT;
          break;
        case GREATER_THAN_OR_EQUAL:
          op = OneDBOpType.GE;
          break;
        case LESS_THAN:
          op = OneDBOpType.LT;
          break;
        case LESS_THAN_OR_EQUAL:
          op = OneDBOpType.LE;
          break;
        case EQUALS:
          op = OneDBOpType.EQ;
          break;
        case NOT_EQUALS:
          op = OneDBOpType.NE;
          break;
        case PLUS:
          op = OneDBOpType.PLUS;
          break;
        case MINUS:
          op = OneDBOpType.MINUS;
          break;
        case TIMES:
          op = OneDBOpType.TIMES;
          break;
        case DIVIDE:
          op = OneDBOpType.DIVIDE;
          break;
        case MOD:
          op = OneDBOpType.MOD;
          break;
        case AND:
          op = OneDBOpType.AND;
          break;
        case OR:
          op = OneDBOpType.OR;
          break;
        default:
          throw new RuntimeException("can't translate " + call);
      }
      List<OneDBExpression> eles =
          Arrays.asList(buildOp(call.operands.get(0)), buildOp(call.operands.get(1)));
      FieldType type = TypeConverter.convert2OneDBType(call.getType().getSqlTypeName());
      return new OneDBOperator(op, type, eles, FuncType.NONE);
    }

    /*
     * add unary operator
     */
    OneDBExpression unary(RexCall call) {
      OneDBOpType op;
      switch (call.getKind()) {
        case AS:
          op = OneDBOpType.AS;
          break;
        case NOT:
          op = OneDBOpType.NOT;
          break;
        case PLUS_PREFIX:
          op = OneDBOpType.PLUS_PRE;
          break;
        case MINUS_PREFIX:
          op = OneDBOpType.MINUS_PRE;
          break;
        case IS_NULL:
          op = OneDBOpType.IS_NULL;
          break;
        case IS_NOT_NULL:
          op = OneDBOpType.IS_NOT_NULL;
          break;
        default:
          throw new RuntimeException("can't translate " + call);
      }
      List<OneDBExpression> eles = Arrays.asList(buildOp(call.operands.get(0)));
      FieldType type = TypeConverter.convert2OneDBType(call.getType().getSqlTypeName());
      return new OneDBOperator(op, type, eles, FuncType.NONE);
    }

    /*
     * translate case
     */
    OneDBExpression caseCall(RexCall call) {
      // in Case Rexcall, oprands are organized as [when, then, when, then, ..., else]
      OneDBOpType op = OneDBOpType.CASE;
      List<OneDBExpression> eles =
          call.operands.stream().map(c -> buildOp(c)).collect(Collectors.toList());
      FieldType type = TypeConverter.convert2OneDBType(call.getType().getSqlTypeName());
      return new OneDBOperator(op, type, eles, FuncType.NONE);
    }

    OneDBExpression searchCall(RexCall call) {
      OneDBOpType op = OneDBOpType.SEARCH;
      List<OneDBExpression> eles =
          call.operands.stream().map(c -> buildOp(c)).collect(Collectors.toList());
      FieldType type = TypeConverter.convert2OneDBType(call.getType().getSqlTypeName());
      return new OneDBOperator(op, type, eles, FuncType.NONE);
    }

    /*
     * translate localref
     */
    OneDBExpression localRef(RexLocalRef node) {
      RexNode local = localNodes.get(node.getIndex());
      // todo: this can be optimized
      return buildOp(local);
    }

    /*
     * translate func
     */
    OneDBExpression scalarFunc(RexCall call) {
      OneDBOpType op = OneDBOpType.SCALAR_FUNC;
      SqlUserDefinedFunction function = (SqlUserDefinedFunction) call.op;
      FuncType func;
      switch (function.getName()) {
        case "ABS":
          func = FuncType.ABS;
          break;
        default:
          throw new RuntimeException("can't translate " + call);
      }
      List<OneDBExpression> eles =
          call.operands.stream().map(r -> buildOp(r)).collect(Collectors.toList());
      FieldType type = TypeConverter.convert2OneDBType(call.getType().getSqlTypeName());
      return new OneDBOperator(op, type, eles, func);
    }
  }
}
