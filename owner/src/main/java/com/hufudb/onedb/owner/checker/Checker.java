package com.hufudb.onedb.owner.checker;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.SchemaManager;
import com.hufudb.onedb.plan.Plan;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.OperatorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Check the modifier of query plan, the owner side implementor only executes plans that pass the
 * check
 */
public class Checker {
  static final Logger LOG = LoggerFactory.getLogger(Checker.class);


  static boolean dominate(Modifier out, Modifier in) {
    // todo: deal with specific protocol condition
    return out.ordinal() >= in.ordinal();
  }

  /**
   * check if ref violates the modifier constraint
   * @param ref
   * @param in
   * @return
   */
  static boolean checkRef(Expression ref, List<Modifier> in) {
    int id = ref.getI32();
    if (id >= in.size() || id < 0) {
      LOG.warn("Column reference out of index");
      return false;
    }
    return dominate(ref.getModifier(), in.get(id));
  }

  static boolean checkExpression(Expression exp, List<Modifier> in) {
    if (exp.getOpType().equals(OperatorType.REF)) {
      return checkRef(exp, in);
    } else if (exp.getOpType().equals(OperatorType.LITERAL)) {
      return true;
    }
    Modifier outModifier = exp.getModifier();
    for (Expression e : exp.getInList()) {
      if (!checkExpression(e, in)) {
        return false;
      }
      // expression's output should be more strict than input
      if (!dominate(outModifier, e.getModifier())) {
        return false;
      }
    }
    return true;
  }

  static boolean checkPlan(Plan plan, List<Modifier> in) {
    for (Expression exp : plan.getSelectExps()) {
      if (!checkExpression(exp, in)) {
        return false;
      }
    }
    for (Expression exp : plan.getAggExps()) {
      if (!checkExpression(exp, in)) {
        return false;
      }
    }
    return true;
  }

  /**
   * check if Plan violates the modifier constraint
   * plan's output should be more strict than input
   * @param plan
   * @param manager
   * @return
   */
  public static boolean check(Plan plan, SchemaManager manager) {
    List<Modifier> in = ImmutableList.of(); // input cols' modifiers
    switch (plan.getPlanType()) {
      case LEAF:
        in = manager.getPublishedSchema(plan.getTableName()).getColumnDescs().stream()
            .map(desc -> desc.getModifier()).collect(Collectors.toList());
        break;
      case UNARY:
        if (!check(plan.getChildren().get(0), manager)) {
          return false;
        }
        in = plan.getChildren().get(0).getOutModifiers();
        break;
      case BINARY:
        if (!check(plan.getChildren().get(0), manager)) {
          return false;
        }
        if (!check(plan.getChildren().get(1), manager)) {
          return false;
        }
        in = new ArrayList<>();
        in.addAll(plan.getChildren().get(0).getOutModifiers());
        in.addAll(plan.getChildren().get(1).getOutModifiers());
        break;
      case EMPTY:
        return true;
      default:
        LOG.warn("Find unsupported plan type {}", plan.getPlanType());
        throw new RuntimeException("Unsupported plan type");
    }
    return checkPlan(plan, in);
  }
}
