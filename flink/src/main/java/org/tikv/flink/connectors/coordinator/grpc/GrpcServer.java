package org.tikv.flink.connectors.coordinator.grpc;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.tikv.common.TiConfiguration;
import org.tikv.flink.connectors.coordinator.CoordinatorSupport;

public class GrpcServer implements CoordinatorSupport {

  private final InetSocketAddress address;
  private final GrpcService service;
  private final Server server;

  public GrpcServer(final String host, final int port, final TiConfiguration conf) {
    this.address = new InetSocketAddress(host, port);
    this.service = new GrpcService(conf);
    this.server = NettyServerBuilder.forAddress(address).addService(service).build();
  }

  public void start() {
    try {
		server.start();
	} catch (final IOException e) {
      throw new RuntimeException(e);
	}
  }

  @Override
  public void close() throws Exception {
    server.shutdown();
  }
}
