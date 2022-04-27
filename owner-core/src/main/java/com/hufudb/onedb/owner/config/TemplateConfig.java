package com.hufudb.onedb.owner.config;

import com.hufudb.onedb.core.config.OneDBConfig;
import com.hufudb.onedb.core.data.utils.POJOPublishedTableInfo;
import com.hufudb.onedb.rpc.grpc.OneDBOwnerInfo;
import com.hufudb.onedb.rpc.grpc.OneDBRpc;
import java.io.File;
import java.util.List;
import java.util.concurrent.Executors;

/*
 * common json config for owner server
 */
public abstract class TemplateConfig implements DBConfig {

  public int threadnum;
  public int id;
  public int port;
  public String hostname;
  public String privatekeypath;
  public String certchainpath;
  public String trustcertpath;
  public String url;
  public String catalog;
  public String user;
  public String passwd;
  public String zkservers;
  public String zkroot;
  public String digest;
  public List<POJOPublishedTableInfo> tables;
  public List<String> endpoints;

  // OwnerService is not generated in this method
  protected OwnerConfig generateConfigInternal() {
    OwnerConfig config = new OwnerConfig();
    config.party = new OneDBOwnerInfo(id, String.format("%s:%d", hostname, port));
    config.port = port;
    config.hostname = hostname;
    if (threadnum > 0) {
      config.threadPool = Executors.newFixedThreadPool(threadnum);
    } else {
      config.threadPool = Executors.newFixedThreadPool(OneDBConfig.SERVER_THREAD_NUM);
    }
    if (privatekeypath != null && certchainpath != null) {
      try {
        config.certChain = new File(certchainpath);
        config.privateKey = new File(privatekeypath);
        config.useTLS = true;
      } catch (Exception e) {
        LOG.error("Fail to read certChainFile or privateKeyFile: {}", e.getMessage());
        config.useTLS = false;
      }
    }
    if (trustcertpath != null) {
      try {
        config.rootCert = new File(trustcertpath);
      } catch (Exception e) {
        LOG.error("Fail to read trustcertFile: {}", e.getMessage());
      }
    }
    config.acrossOwnerService = new OneDBRpc(config.party, config.threadPool);
    return config;
  }
}
