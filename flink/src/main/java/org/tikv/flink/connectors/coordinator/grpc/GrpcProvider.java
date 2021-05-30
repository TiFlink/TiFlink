package org.tikv.flink.connectors.coordinator.grpc;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.flink.connectors.coordinator.Coordinator;
import org.tikv.flink.connectors.coordinator.CoordinatorProvider;

public class GrpcProvider implements CoordinatorProvider, AutoCloseable {
  private static Logger logger = LoggerFactory.getLogger(GrpcProvider.class);

  private final InetSocketAddress address;
  private final GrpcService service;
  private final Server server;

  public GrpcProvider(final String host, final int port, final TiConfiguration conf) {
    this.address = new InetSocketAddress(host, port);
    this.service = new GrpcService(conf);
    this.server = NettyServerBuilder.forAddress(address).addService(service).build();
    try {
      logger.info("starting server at: {}", port);
      server.start();
    } catch (final IOException e) {
      throw new RuntimeException("Unable to start server", e);
    }
  }

  @Override
  public Coordinator createCoordinator() {
    return new GrpcClient(address);
  }

  @Override
  public void close() throws Exception {
    server.shutdown();
  }
}
