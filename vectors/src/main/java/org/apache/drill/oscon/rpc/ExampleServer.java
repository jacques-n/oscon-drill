package org.apache.drill.oscon.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.rpc.BasicServer;
import org.apache.drill.exec.rpc.RemoteConnection;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.oscon.ExampleProtos.AddMessage;
import org.apache.drill.oscon.ExampleProtos.ExampleRpcType;
import org.apache.drill.oscon.ExampleProtos.Handshake;
import org.apache.drill.oscon.ExampleProtos.NodeMode;
import org.apache.drill.oscon.ExampleProtos.ResponseBundledMessage;
import org.apache.drill.oscon.ExampleProtos.ResponseVectorMessage;
import org.apache.drill.oscon.ExampleProtos.SubmitBundledMessage;
import org.apache.drill.oscon.ExampleProtos.SubmitVectorMessage;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

public class ExampleServer extends BasicServer<ExampleRpcType, ExampleServer.ServerConnection> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExampleServer.class);

  
  private BufferAllocator localAlloc;

  public ExampleServer(BufferAllocator alloc, EventLoopGroup eventLoopGroup) {
    super(ExampleConfig.CONFIG, alloc.getUnderlyingAllocator(), eventLoopGroup);
    this.localAlloc = alloc;
  }

  @Override
  protected ServerHandshakeHandler<?> getHandshakeHandler(ServerConnection connection) {
    return new ServerHandshakeHandler<Handshake>(ExampleRpcType.HANDSHAKE, Handshake.PARSER) {
      @Override
      public MessageLite getHandshakeResponse(Handshake inbound) throws Exception {
        if (inbound.getMode() != NodeMode.CLIENT)
          throw new RpcException("Connections should only come from client nodes.");
        return Handshake.newBuilder().setMode(NodeMode.SERVER).setVersion(ExampleConfig.VERSION).build();
      }
    };
  }

  @Override
  protected MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    switch (rpcType) {
    case ExampleRpcType.RESPONSE_BUNDLE_VALUE:
      return ResponseBundledMessage.getDefaultInstance();
    case ExampleRpcType.RESPONSE_VECTOR_VALUE:
      return ResponseVectorMessage.getDefaultInstance();
    case ExampleRpcType.SUBMIT_BUNDLE_VALUE:
      return SubmitBundledMessage.getDefaultInstance();
    case ExampleRpcType.SUBMIT_VECTOR_VALUE:
      return SubmitVectorMessage.getDefaultInstance();
    default:
      throw new IllegalStateException();
    }
  }

  @Override
  protected Response handle(ServerConnection connection, int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException {
    switch (rpcType) {
    case ExampleRpcType.SUBMIT_BUNDLE_VALUE:
      SubmitBundledMessage bundle = null;
      try {
        bundle = SubmitBundledMessage.PARSER.parseFrom(new ByteBufInputStream(pBody));
      } catch (InvalidProtocolBufferException e) {
        throw new RpcException(e);
      }

      ResponseBundledMessage.Builder bldr = ResponseBundledMessage.newBuilder();
      for (AddMessage m : bundle.getMessageList()) {
        bldr.addMessage(m.getA() + m.getB());
      }
      
      return new Response(ExampleRpcType.RESPONSE_BUNDLE, bldr.build());

    case ExampleRpcType.SUBMIT_VECTOR_VALUE:

      int valueCount = 0;
      try {
        SubmitVectorMessage vm = SubmitVectorMessage.PARSER.parseFrom(new ByteBufInputStream(pBody));
        valueCount = vm.getValueCount();
      } catch (InvalidProtocolBufferException e) {
        throw new RpcException(e);
      }

      IntVector a = new IntVector(null, this.localAlloc);
      IntVector b = new IntVector(null, this.localAlloc);

      int shift = a.load(valueCount, dBody);
      dBody = dBody.slice(shift, dBody.capacity() - shift);
      b.load(valueCount, dBody);
      
      

      IntVector.Accessor aa = a.getAccessor();
      IntVector.Accessor bb = b.getAccessor();

      IntVector output = new IntVector(null, this.localAlloc);
      output.allocateNew(valueCount);
      IntVector.Mutator outm = output.getMutator();

      for (int i = 0; i < valueCount; i++) {
        outm.set(i, aa.get(i) + bb.get(i));
      }
      outm.setValueCount(valueCount);
      a.clear();
      b.clear();
      return new Response(ExampleRpcType.RESPONSE_VECTOR, ResponseVectorMessage.newBuilder().setValueCount(valueCount)
          .build(), output.getBuffers());

    }
    throw new RpcException("Unhandled RPC submission.");
  }
  
  @Override
  public ExampleServer.ServerConnection initRemoteConnection(Channel channel) {
    return new ServerConnection(channel);
  }

  public static class ServerConnection extends RemoteConnection{
    public ServerConnection(Channel channel) {
      super(channel);
    }
  }
  
}
