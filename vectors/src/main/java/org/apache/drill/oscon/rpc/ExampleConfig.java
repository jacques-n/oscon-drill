package org.apache.drill.oscon.rpc;

import org.apache.drill.exec.rpc.RpcConfig;
import org.apache.drill.oscon.ExampleProtos.ExampleRpcType;
import org.apache.drill.oscon.ExampleProtos.Handshake;
import org.apache.drill.oscon.ExampleProtos.ResponseBundledMessage;
import org.apache.drill.oscon.ExampleProtos.ResponseVectorMessage;
import org.apache.drill.oscon.ExampleProtos.SubmitBundledMessage;
import org.apache.drill.oscon.ExampleProtos.SubmitVectorMessage;

public class ExampleConfig {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExampleConfig.class);

  public static final long VERSION = 1;
  
  public static final RpcConfig CONFIG = RpcConfig.newBuilder("Example")
      .add(ExampleRpcType.HANDSHAKE, Handshake.class, ExampleRpcType.HANDSHAKE, Handshake.class)
      .add(ExampleRpcType.SUBMIT_VECTOR, SubmitVectorMessage.class, ExampleRpcType.RESPONSE_VECTOR, ResponseVectorMessage.class)
      .add(ExampleRpcType.SUBMIT_BUNDLE, SubmitBundledMessage.class, ExampleRpcType.RESPONSE_BUNDLE, ResponseBundledMessage.class)
      .build();
}
