package com.hufudb.onedb.owner.mysql;

import com.google.gson.Gson;
import com.hufudb.onedb.owner.OwnerServer;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class MysqlServer extends OwnerServer {

  public MysqlServer(MysqlConfig config) throws IOException {
    super(config.port, new MysqlService(config), null);
  }

  public MysqlServer(int port, MysqlService service) throws IOException {
    super(port, service, null);
  }

  public static void main(String[] args) {
    Options options = new Options();
    Option config = new Option("c", "config", true, "mysql config");
    config.setRequired(true);
    options.addOption(config);
    CommandLineParser parser = new DefaultParser();
    Gson gson = new Gson();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
      Reader reader = Files.newBufferedReader(Paths.get(cmd.getOptionValue("config")));
      MysqlConfig pConfig = gson.fromJson(reader, MysqlConfig.class);
      MysqlServer server = new MysqlServer(pConfig);
      server.start();
      server.blockUntilShutdown();
    } catch (ParseException | IOException | InterruptedException e) {
      System.out.println(e.getMessage());
      System.exit(1);
    }
  }
}
