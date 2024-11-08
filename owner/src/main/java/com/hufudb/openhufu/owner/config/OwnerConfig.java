package com.hufudb.openhufu.owner.config;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import com.hufudb.openhufu.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.openhufu.mpc.ProtocolExecutor;
import com.hufudb.openhufu.mpc.ProtocolType;
import com.hufudb.openhufu.owner.adapter.Adapter;
import com.hufudb.openhufu.rpc.Party;
import com.hufudb.openhufu.rpc.grpc.OpenHuFuRpc;
import io.grpc.ChannelCredentials;
import io.grpc.ServerCredentials;
import redis.clients.jedis.Jedis;

public class OwnerConfig {
  public Party party;
  public int port;
  public String hostname;
  public ExecutorService threadPool;
  public Adapter adapter;
  public OpenHuFuRpc acrossOwnerRpc;
  public boolean useTLS;
  public ServerCredentials serverCerts;
  public ChannelCredentials clientCerts;
  public List<PojoPublishedTableSchema> tables;
  public Map<ProtocolType, ProtocolExecutor> librarys;
  public String implementorConfigPath;

  public Jedis jedis;
  public OwnerConfig() {}

  public OwnerConfig(Party party, int port, String hostname, ExecutorService threadPool,
      Adapter adapter, OpenHuFuRpc acrossOwnerRpc, boolean useTLS, ServerCredentials serverCerts,
      ChannelCredentials clientCerts, List<PojoPublishedTableSchema> tables) {
    this.party = party;
    this.port = port;
    this.hostname = hostname;
    this.threadPool = threadPool;
    this.adapter = adapter;
    this.acrossOwnerRpc = acrossOwnerRpc;
    this.useTLS = useTLS;
    this.serverCerts = serverCerts;
    this.clientCerts = clientCerts;
    this.tables = tables;
  }
}
