package com.hufudb.onedb.owner.adapter.mysql;

import java.sql.Connection;
import java.sql.Statement;
import com.hufudb.onedb.expression.Translator;
import com.hufudb.onedb.owner.adapter.AdapterTypeConverter;
import com.hufudb.onedb.owner.adapter.jdbc.JDBCAdapter;

public class MysqlAdapter extends JDBCAdapter {
  MysqlAdapter(String catalog, Connection connection, Statement statement,
      AdapterTypeConverter converter, Translator translator) {
    super(catalog, connection, statement, converter, translator);
  }
}
