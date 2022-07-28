package com.hufudb.onedb.owner.adapter.postgis;

import com.hufudb.onedb.expression.AggFuncType;
import com.hufudb.onedb.expression.BasicTranslator;
import com.hufudb.onedb.expression.ExpressionUtils;
import com.hufudb.onedb.expression.ScalarFuncType;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import java.util.List;
import java.util.stream.Collectors;

public class PostgisTranslator extends BasicTranslator {

  public PostgisTranslator() {
    super();
  }

  @Override
  protected String scalarFunc(Expression exp) {
    ScalarFuncType func = ScalarFuncType.of(exp.getI32());
    List<String> inputs = exp.getInList().stream().map(e -> translate(e)).collect(Collectors.toList());
    switch (func) {
      case ABS:
        if (inputs.size() != 1) {
          throw new RuntimeException("ABS need 1 arguements, but given " + inputs.size());
        }
        return String.format("ABS(%s)", inputs.get(0));
      case POINT:
        if (inputs.size() != 2) {
          throw new RuntimeException("Point need 2 arguments, but given " + inputs.size());
        }
        return String.format("'SRID=4326;POINT(%s %s)'", inputs.get(0), inputs.get(1));
      case DWITHIN:
        if (inputs.size() != 3) {
          throw new RuntimeException("DWithin need 3 arguments, but given " + inputs.size());
        }
        return String.format("ST_DWithin(%s, %s, %s)", inputs.get(0), inputs.get(1), inputs.get(2));
      case DISTANCE:
        if (inputs.size() != 2) {
          throw new RuntimeException("Distance need 2 arguments, but given " + inputs.size());
        }
        return String.format("%s <-> %s", inputs.get(0), inputs.get(1));
      default:
        throw new RuntimeException("can't translate scalarFunc " + exp);
    }
  }

  @Override
  protected String aggregateFunc(Expression exp) {
    AggFuncType type = AggFuncType.of(exp.getI32());
    List<Integer> inputRefs = ExpressionUtils.getAggInputs(exp);
    switch (type) {
      case GROUPKEY:
        return inputStrs.get(inputRefs.get(0));
      case SUM:
        return String.format("SUM(%s)", inputStrs.get(inputRefs.get(0)));
      case COUNT:
        if (inputRefs.isEmpty()) {
          return String.format("COUNT(*)");
        } else if (inputRefs.size() == 1) {
          return String.format("COUNT(%s)", inputStrs.get(inputRefs.get(0)));
        } else {
          List<String> inputs = inputRefs.stream().map(ref -> inputStrs.get(ref))
              .collect(Collectors.toList());
          return String.format("COUNT((%s))", String.join(",", inputs));
        }
      case AVG:
        return String.format("AVG(%s)", inputStrs.get(inputRefs.get(0)));
      case MAX:
        return String.format("MAX(%s)", inputStrs.get(inputRefs.get(0)));
      case MIN:
        return String.format("MIN(%s)", inputStrs.get(inputRefs.get(0)));
      case KNN:
        throw new RuntimeException("KNN is not implemented in com.hufudb.onedb.expression.PostgistTranslator yet!");
        // return String.format("KNN(%s)", inputStrs.get(inputRefs.get(0)));
      default:
        throw new RuntimeException("can't translate aggFunc " + exp);
    }
  }
}
