package org.apache.drill.oscon.rpc;

import io.netty.buffer.ByteBuf;

import org.apache.drill.exec.rpc.BasicClientWithConnection.ServerConnection;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RpcConnectionHandler;
import org.apache.drill.exec.rpc.RpcException;

import com.google.common.util.concurrent.AbstractCheckedFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Used to manage the various stages of connection between client and server. First we have to establish the connection,
 * then we have to do the handshake and validate the response. Then we can return success
 */
public class ClientConnectFuture extends AbstractCheckedFuture<Void, RpcException> implements
    RpcConnectionHandler<ServerConnection>, DrillRpcFuture<Void> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClientConnectFuture.class);

  protected ClientConnectFuture() {
    super(SettableFuture.<Void> create());
  }

  @Override
  public void connectionSucceeded(ServerConnection connection) {
    getInner().set(null);
  }

  @Override
  public void connectionFailed(FailureType type, Throwable t) {
    getInner().setException(
        new RpcException(String.format("Failure connecting to server. Failure of type %s.", type.name()), t));
  }

  private SettableFuture<Void> getInner() {
    return (SettableFuture<Void>) delegate();
  }

  @Override
  protected RpcException mapException(Exception e) {
    return RpcException.mapException(e);
  }

  @Override
  public ByteBuf getBuffer() {
    return null;
  }

}
