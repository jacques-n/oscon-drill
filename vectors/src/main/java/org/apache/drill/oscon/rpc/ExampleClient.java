package org.apache.drill.oscon.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoopGroup;

import org.apache.drill.exec.rpc.BasicClientWithConnection;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.oscon.ExampleProtos.ExampleRpcType;
import org.apache.drill.oscon.ExampleProtos.Handshake;
import org.apache.drill.oscon.ExampleProtos.NodeMode;
import org.apache.drill.oscon.ExampleProtos.ResponseBundledMessage;
import org.apache.drill.oscon.ExampleProtos.ResponseVectorMessage;
import org.apache.drill.oscon.ExampleProtos.SubmitBundledMessage;
import org.apache.drill.oscon.ExampleProtos.SubmitVectorMessage;

import com.google.protobuf.MessageLite;

public class ExampleClient extends BasicClientWithConnection<ExampleRpcType, Handshake, Handshake> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExampleClient.class);

  public ExampleClient(ByteBufAllocator alloc, EventLoopGroup eventLoopGroup) {
    super(ExampleConfig.CONFIG, alloc, eventLoopGroup, ExampleRpcType.HANDSHAKE, Handshake.class, Handshake.PARSER);
  }

  
  public ClientConnectFuture connectAsClient(String host, int port) {
    ClientConnectFuture fut = new ClientConnectFuture();
    super.connectAsClient(fut, Handshake.newBuilder().setMode(NodeMode.CLIENT).setVersion(ExampleConfig.VERSION).build(), host, port);
    return fut;
  }

  @Override
  protected Response handle(int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException {
    throw new UnsupportedOperationException("This client doesn't support inbound messages.");
  }
  
  public DrillRpcFuture<ResponseBundledMessage> sendBundle(SubmitBundledMessage msg) throws RpcException{
    return this.send(ExampleRpcType.SUBMIT_BUNDLE, msg, ResponseBundledMessage.class);
  }

  public DrillRpcFuture<ResponseVectorMessage> sendVector(SubmitVectorMessage msg, ByteBuf...bufs) throws RpcException{
    return this.send(ExampleRpcType.SUBMIT_VECTOR, msg, ResponseVectorMessage.class, bufs);
  }
  
  @Override
  protected void validateHandshake(Handshake validateHandshake) throws RpcException {
    if (validateHandshake.getMode() != NodeMode.SERVER)
      throw new RpcException("Attempted to connect to client from another client.");
    if (validateHandshake.getVersion() != ExampleConfig.VERSION)
      throw new RpcException(String.format("Unexpected Rpc version.  Expected %d but received %d.",
          ExampleConfig.VERSION, validateHandshake.getVersion()));
  }

  @Override
  protected void finalizeConnection(Handshake handshake, org.apache.drill.exec.rpc.BasicClientWithConnection.ServerConnection connection) {
  }

  @Override
  protected MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    switch(rpcType){
    case ExampleRpcType.RESPONSE_BUNDLE_VALUE:
      return ResponseBundledMessage.getDefaultInstance();
    case ExampleRpcType.RESPONSE_VECTOR_VALUE:
      return ResponseVectorMessage.getDefaultInstance();
    default: 
      throw new RpcException(String.format("Unknown rpc type %d.", rpcType));
    }
  }

}
