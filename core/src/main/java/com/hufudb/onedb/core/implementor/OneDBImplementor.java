package com.hufudb.onedb.core.implementor;

import java.util.List;
import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.implementor.plaintext.PlaintextImplementor;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.sql.context.OneDBContext;
import com.hufudb.onedb.core.sql.context.OneDBLeafContext;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.rel.OneDBOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface OneDBImplementor {
  static final Logger LOG = LoggerFactory.getLogger(OneDBImplementor.class);

  QueryableDataSet implement(OneDBContext context);

  QueryableDataSet join(QueryableDataSet left, QueryableDataSet right, OneDBJoinInfo joinInfo);

  QueryableDataSet filter(QueryableDataSet in, List<OneDBExpression> filters);

  QueryableDataSet project(QueryableDataSet in, List<OneDBExpression> projects);

  QueryableDataSet aggregate(QueryableDataSet in, List<Integer> groups, List<OneDBExpression> aggs,
      List<FieldType> inputTypes);

  QueryableDataSet sort(QueryableDataSet in, List<OneDBOrder> orders);

  QueryableDataSet leafQuery(OneDBLeafContext leaf);

  public static OneDBImplementor getImplementor(OneDBContext context, OneDBClient client) {
    switch (context.getContextLevel()) {
      case PUBLIC:
        return new PlaintextImplementor(client);
      default:
        LOG.error("No implementor found for Level {}", context.getContextLevel().name());
        throw new UnsupportedOperationException(String.format("No implementor found for Level %s", context.getContextLevel().name()));
    }
  }
}
