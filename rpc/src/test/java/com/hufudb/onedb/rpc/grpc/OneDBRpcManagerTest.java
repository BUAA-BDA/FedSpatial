package com.hufudb.onedb.rpc.grpc;

import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.rpc.Party;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import io.grpc.Channel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;

@RunWith(JUnit4.class)
public class OneDBRpcManagerTest {
  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  static Random rand = new Random();

  List<byte[]> generatePayloads(int n, int size) {
    List<byte[]> payloads = new ArrayList<>();
    for (int i = 0; i < n; ++i) {
      byte[] payload = new byte[size];
      for (int j = 0; j < size; ++j) {
        payload[j] = (byte)rand.nextInt();
      }
      payloads.add(payload);
    }
    return payloads;
  }

  DataPacket generateDataPacket(int senderId, int receiverId) {
    DataPacketHeader header = new DataPacketHeader(1, 2, 3, senderId, receiverId);
    List<byte[]> payloads = generatePayloads(2, 10);
    return DataPacket.fromByteArrayList(header, payloads);
  }

  @Test
  public void OneDBRpcTest() throws Exception {
    String ownerName0 = InProcessServerBuilder.generateName();
    String ownerName1 = InProcessServerBuilder.generateName();
    System.out.print("");
    Party owner0 = new OneDBOwnerInfo(0, ownerName0);
    Party owner1 = new OneDBOwnerInfo(1, ownerName1);
    List<Party> parties = ImmutableList.of(
      owner0, owner1
    );
    List<Channel> channels = Arrays.asList(
      grpcCleanup.register(InProcessChannelBuilder.forName(ownerName0).directExecutor().build()),
      grpcCleanup.register(InProcessChannelBuilder.forName(ownerName1).directExecutor().build())
    );
    OneDBRpcManager manager = new OneDBRpcManager(parties, channels);
    OneDBRpc rpc0 = (OneDBRpc) manager.getRpc(0);
    OneDBRpc rpc1 = (OneDBRpc) manager.getRpc(1);
    Server server0 = InProcessServerBuilder.forName(ownerName0).directExecutor().addService(rpc0.getgRpcService()).build().start();
    Server server1 = InProcessServerBuilder.forName(ownerName1).directExecutor().addService(rpc1.getgRpcService()).build().start();
    grpcCleanup.register(server0);
    grpcCleanup.register(server1);
    rpc0.connect();
    rpc1.connect();
    DataPacket packet0 = generateDataPacket(0, 1);
    DataPacket packet1 = generateDataPacket(1, 0);
    rpc0.send(packet0);
    rpc1.send(packet1);
    DataPacketHeader headerfrom0 = packet0.getHeader();
    DataPacketHeader headerfrom1 = packet1.getHeader();
    DataPacket r0 = rpc0.receive(headerfrom1);
    DataPacket r1 = rpc1.receive(headerfrom0);
    assertTrue("rpc0 receive wrong message", r0.equals(packet1));
    assertTrue("rpc0 receive wrong message", r1.equals(packet0));
    rpc0.disconnect();
    rpc1.disconnect();
    Thread.sleep(1000);
  }
}
