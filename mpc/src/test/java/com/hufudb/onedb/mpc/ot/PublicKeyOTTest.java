package com.hufudb.onedb.mpc.ot;

import static org.junit.Assert.assertEquals;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.mpc.ProtocolType;
import com.hufudb.onedb.mpc.codec.OneDBCodec;
import com.hufudb.onedb.mpc.random.BasicRandom;
import com.hufudb.onedb.mpc.random.OneDBRandom;
import com.hufudb.onedb.rpc.Party;
import com.hufudb.onedb.rpc.grpc.OneDBOwnerInfo;
import com.hufudb.onedb.rpc.grpc.OneDBRpcManager;
import com.hufudb.onedb.rpc.grpc.OneDBRpc;
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
public class PublicKeyOTTest {
  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  public static OneDBRandom rand = new BasicRandom();

  public List<byte[]> encode4Sender(List<String> secrets) {
    return secrets.stream().map(s -> s.getBytes()).collect(Collectors.toList());
  }

  public List<byte[]> encode4Receiver(int select) {
    return ImmutableList.of(OneDBCodec.encodeInt(2), OneDBCodec.encodeInt(select));
  }

  @Test
  public void testPublicKeyOT() throws Exception {
    try {
      String ownerName0 = InProcessServerBuilder.generateName();
      String ownerName1 = InProcessServerBuilder.generateName();
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
      PublicKeyOT otSender = new PublicKeyOT(rpc0);
      PublicKeyOT otReceiver = new PublicKeyOT(rpc1);
      List<String> secrets = Arrays.asList("Alice", "Bob", "Jerry", "Tom");
      int tid = rand.nextInt(secrets.size());
      String expect = secrets.get(tid);
      ExecutorService service = Executors.newFixedThreadPool(2);
      Future<List<byte[]>> senderRes = service.submit(
        new Callable<List<byte[]>>() {
          @Override
          public List<byte[]> call() throws Exception {
            return otSender.run(0, ImmutableList.of(0, 1), encode4Sender(secrets), 0, 1);
          }
        }
      );
      Future<List<byte[]>> receiverRes = service.submit(
        new Callable<List<byte[]>>() {
        @Override
        public List<byte[]> call() throws Exception {
          return otReceiver.run(0, ImmutableList.of(0, 1), encode4Receiver(tid), 0, 1);
        }
      });
      List<byte[]> result = receiverRes.get();
      String actual = new String(result.get(0));
      assertEquals(expect, actual);
      rpc0.disconnect();
      rpc1.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }
}
