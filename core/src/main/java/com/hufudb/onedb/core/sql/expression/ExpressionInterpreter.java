package com.hufudb.onedb.core.sql.expression;

import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Row;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.List;

// plaintext interpreter for onedb expression
public class ExpressionInterpreter {

  public static Comparable implement(Row row, OneDBExpression e) {
    switch (e.getOpType()) {
      case REF:
        return implementRef(row, (OneDBReference) e);
      case LITERAL:
        return implementLiteral(row, (OneDBLiteral) e);
      case AGG_FUNC:
        throw new UnsupportedOperationException("aggregate not support in interpereter");
      default:
        return implementOperator(row, (OneDBOperator) e);
    }
  }

  static Comparable implementRef(Row row, OneDBReference ref) {
    return (Comparable) row.getObject(ref.getIdx());
  }

  static Comparable implementLiteral(Row row, OneDBLiteral literal) {
    return (Comparable) literal.getValue();
  }

  static Comparable implementOperator(Row row, OneDBOperator op) {
    List<OneDBExpression> eles = op.getInputs();
    switch (op.getOpType()) {
      // binary
      case GT:
        return implement(row, eles.get(0)).compareTo(implement(row, eles.get(1))) > 0;
      case GE:
        return implement(row, eles.get(0)).compareTo(implement(row, eles.get(1))) >= 0;
      case LT:
        return implement(row, eles.get(0)).compareTo(implement(row, eles.get(1))) < 0;
      case LE:
        return implement(row, eles.get(0)).compareTo(implement(row, eles.get(1))) <= 0;
      case EQ:
        return implement(row, eles.get(0)).compareTo(implement(row, eles.get(1))) == 0;
      case NE:
        return implement(row, eles.get(0)).compareTo(implement(row, eles.get(1))) != 0;
      case PLUS:
        return number(implement(row, eles.get(0))).add(number(implement(row, eles.get(1))));
      case MINUS:
        return number(implement(row, eles.get(0))).subtract(number(implement(row, eles.get(1))));
      case TIMES:
        return number(implement(row, eles.get(0))).multiply(number(implement(row, eles.get(1))));
      case DIVIDE:
        return number(implement(row, eles.get(0)))
            .divide(number(number(implement(row, eles.get(1)))), MathContext.DECIMAL64);
      case MOD:
        return number(implement(row, eles.get(0))).remainder(number(implement(row, eles.get(1))));
      case AND:
        return ((Boolean) implement(row, eles.get(0))) && ((Boolean) implement(row, eles.get(1)));
      case OR:
        return ((Boolean) implement(row, eles.get(0))) || ((Boolean) implement(row, eles.get(1)));
      // unary
      case AS:
        return implement(row, eles.get(0));
      case PLUS_PRE:
        return number(implement(row, eles.get(0))).plus();
      case MINUS_PRE:
        return number(implement(row, eles.get(0))).negate();
      case IS_NULL:
        return implement(row, eles.get(0)) == null;
      case IS_NOT_NULL:
        return implement(row, eles.get(0)) != null;
      case NOT:
        return !(Boolean) implement(row, eles.get(0));
      case CASE:
        for (int i = 1; i < eles.size(); i += 2) {
          if ((Boolean) implement(row, eles.get(i - 1))) {
            return implement(row, eles.get(i));
          }
        }
        return implement(row, eles.get(eles.size() - 1));
      // todo: support scalar functions
      default:
        throw new UnsupportedOperationException("operator not support in intereperter");
    }
  }

  private static BigDecimal number(Comparable comparable) {
    return comparable instanceof BigDecimal ? (BigDecimal) comparable
        : comparable instanceof BigInteger ? new BigDecimal((BigInteger) comparable)
            : comparable instanceof Long || comparable instanceof Integer
                || comparable instanceof Short ? new BigDecimal(((Number) comparable).longValue())
                    : new BigDecimal(((Number) comparable).doubleValue());
  }

  public static Comparable cast(Comparable in, final FieldType type) {
    switch (type) {
      case STRING:
        return in;
      case BOOLEAN:
        return in;
      case BYTE:
        return number(in).byteValue();
      case SHORT:
        return number(in).shortValue();
      case INT:
        return number(in).intValue();
      case DATE:
      case TIME:
      case TIMESTAMP:
      case LONG:
        return number(in).longValue();
      case FLOAT:
        return number(in).floatValue();
      case DOUBLE:
        return number(in).doubleValue();
      case POINT:
        return in;
      default:
        throw new UnsupportedOperationException("field type not support");
    }
  }
}
