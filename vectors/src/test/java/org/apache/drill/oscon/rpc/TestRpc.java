package org.apache.drill.oscon.rpc;
import io.netty.buffer.ByteBuf;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.TimeUnit;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.oscon.ExampleProtos.AddMessage;
import org.apache.drill.oscon.ExampleProtos.ResponseBundledMessage;
import org.apache.drill.oscon.ExampleProtos.ResponseVectorMessage;
import org.apache.drill.oscon.ExampleProtos.SubmitBundledMessage;
import org.apache.drill.oscon.ExampleProtos.SubmitVectorMessage;
import org.junit.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ObjectArrays;

public class TestRpc {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestRpc.class);

  static final int VALUES_PER_BATCH = 15000;
  static final int OUTER_ITERS = 30;
  static final int INNER_ITERS = 100;

  @Test 
  public void testBundled() throws Exception{
    NioEventLoopGroup serverLoop = new NioEventLoopGroup(1, new NamedThreadFactory("server-"));
    NioEventLoopGroup clientLoop = new NioEventLoopGroup(1, new NamedThreadFactory("client-"));
    BufferAllocator alloc = BufferAllocator.getAllocator(DrillConfig.create());
    

    try (ExampleServer server = new ExampleServer(alloc, serverLoop);
        ExampleClient client = new ExampleClient(alloc.getUnderlyingAllocator(), clientLoop);) {
      int serverPort = server.bind(2022);
      client.connectAsClient("127.0.0.1", serverPort).get();
      for (int outerIndex = 0; outerIndex < OUTER_ITERS; outerIndex++) {
        Stopwatch watch = new Stopwatch().start();
        
        @SuppressWarnings("unchecked")
        DrillRpcFuture<ResponseBundledMessage>[] responses = new DrillRpcFuture[INNER_ITERS];
        
        
        
        for (int innerIndex = 0; innerIndex < INNER_ITERS; innerIndex++) {
          SubmitBundledMessage.Builder bldr = SubmitBundledMessage.newBuilder();

          for (int valueIndex = 0; valueIndex < VALUES_PER_BATCH; valueIndex++) {
            bldr.addMessage(AddMessage.newBuilder().setA(valueIndex+1).setB(-valueIndex));
          }

          responses[innerIndex] = client.sendBundle(bldr.build());
        }

        long output = 0;

        for (int innerIndex = 0; innerIndex < INNER_ITERS; innerIndex++) {
          ResponseBundledMessage msg = responses[innerIndex].checkedGet();
          for (int msgIndex = 0; msgIndex < msg.getMessageCount(); msgIndex++) {
            output += msg.getMessage(msgIndex);
          }
        }

        watch.stop();
        try {
          assert output == INNER_ITERS * VALUES_PER_BATCH;
        } catch (AssertionError e) {
          System.out.println("Exception on loop " + outerIndex + ", output was " + output);
          throw e;
        }
        System.out.print(String.format("%d\t", watch.elapsed(TimeUnit.MILLISECONDS)));
        if ((outerIndex+1) % 15 == 0) {
          System.out.println();
        }

      }
      System.out.println();
      System.out.println(String.format("Bundled Data Per Batch: %f mb sent, %f mb received.", VALUES_PER_BATCH * 1.0f * 8 * INNER_ITERS/ 1024.0/1024.0, VALUES_PER_BATCH * 4 * INNER_ITERS/1024.0/1024.0));        

    } catch (Exception | Error e) {
      Thread.sleep(1000);
      throw e;
    }

  }
  
  @Test
  public void testVector() throws Exception {
    NioEventLoopGroup serverLoop = new NioEventLoopGroup(1, new NamedThreadFactory("server-"));
    NioEventLoopGroup clientLoop = new NioEventLoopGroup(1, new NamedThreadFactory("client-"));
    BufferAllocator alloc = BufferAllocator.getAllocator(DrillConfig.create());
    

    try (ExampleServer server = new ExampleServer(alloc, serverLoop);
        ExampleClient client = new ExampleClient(alloc.getUnderlyingAllocator(), clientLoop);) {
      logger.debug("Starting test.");
      int serverPort = server.bind(2022);
      client.connectAsClient("127.0.0.1", serverPort).get();
      for (int outerIndex = 0; outerIndex < OUTER_ITERS; outerIndex++) {
        Stopwatch watch = new Stopwatch().start();
        
        @SuppressWarnings("unchecked")
        DrillRpcFuture<ResponseVectorMessage>[] responses = new DrillRpcFuture[INNER_ITERS];
        
        for (int innerIndex = 0; innerIndex < INNER_ITERS; innerIndex++) {

          IntVector a = new IntVector(null, alloc);
          IntVector b = new IntVector(null, alloc);

          a.allocateNew(VALUES_PER_BATCH);
          b.allocateNew(VALUES_PER_BATCH);
          IntVector.Mutator aa = a.getMutator();
          IntVector.Mutator bb = b.getMutator();

          for (int valueIndex = 0; valueIndex < VALUES_PER_BATCH; valueIndex++) {
             aa.set(valueIndex, valueIndex + 1);
             bb.set(valueIndex, -valueIndex);
          }

          aa.setValueCount(VALUES_PER_BATCH);
          bb.setValueCount(VALUES_PER_BATCH);

          responses[innerIndex] = client.sendVector(SubmitVectorMessage.newBuilder() //
              .setValueCount(VALUES_PER_BATCH) //
              .build(), ObjectArrays.concat(a.getBuffers(), b.getBuffers(), ByteBuf.class));
        }

        long output = 0;
        IntVector incoming = new IntVector(null, alloc);
        for (int innerIndex = 0; innerIndex < INNER_ITERS; innerIndex++) {
          ResponseVectorMessage msg = responses[innerIndex].checkedGet();
          incoming.load(msg.getValueCount(), responses[innerIndex].getBuffer());
          IntVector.Accessor accessor = incoming.getAccessor();
          for (int valueIndex = 0; valueIndex < msg.getValueCount(); valueIndex++) {
            output += accessor.get(valueIndex);
          }
          
          // drop incoming reference to buffer.
          incoming.clear();
          
          // since we used a future, rather than a listener, we need to drop the future's reference as well.  This is a sharp edge.
          responses[innerIndex].getBuffer().release();
        }

        watch.stop();
        try {
          assert output == INNER_ITERS * VALUES_PER_BATCH;
        } catch (AssertionError e) {
          System.out.println("Exception on loop " + outerIndex + ", output was " + output);
          throw e;
        }
        System.out.print(String.format("%d\t", watch.elapsed(TimeUnit.MILLISECONDS)));
        if ((outerIndex+1) % 15 == 0) {
          System.out.println();
        }

        incoming.close();

      }
      System.out.println();
      System.out.println(String.format("Vector Data Per Batch: %f mb sent, %f mb received.", VALUES_PER_BATCH * 1.0f * 8 * INNER_ITERS/ 1024.0/1024.0, VALUES_PER_BATCH * 4 * INNER_ITERS/1024.0/1024.0));        

    } catch (Exception | Error e) {
      Thread.sleep(1000);
      throw e;
    }

  }

}
