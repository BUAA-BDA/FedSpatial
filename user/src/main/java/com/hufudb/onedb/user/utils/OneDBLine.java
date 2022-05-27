package com.hufudb.onedb.user.utils;

import java.io.IOException;
import java.io.InputStream;
import sqlline.BuiltInProperty;
import sqlline.SqlLine;
import sqlline.SqlLineOpts;

public class OneDBLine extends SqlLine {
  private static String LOGO = String.join
  ("\n",
  "   ____                _____   ____  ",
  "  / __ \\              |  __ \\ |  _ \\ ",
  " | |  | | _ __    ___ | |  | || |_) |",
  " | |  | || '_ \\  / _ \\| |  | ||  _ < ",
  " | |__| || | | ||  __/| |__| || |_) |",
  "  \\____/ |_| |_| \\___||_____/ |____/ "
  );

  public static Status start(String[] args, InputStream inputStream, boolean saveHistory)
      throws IOException {
    System.out.println(LOGO);
    OneDBLine onedbline = new OneDBLine();
    onedbline.getOpts().set(BuiltInProperty.PROMPT, "onedb>");
    onedbline.getOpts().set(BuiltInProperty.ISOLATION, "TRANSACTION_NONE");
    Status status = onedbline.begin(args, inputStream, saveHistory);
    if (!Boolean.getBoolean(SqlLineOpts.PROPERTY_NAME_EXIT)) {
      System.exit(status.ordinal());
    }
    return status;
  }
}
