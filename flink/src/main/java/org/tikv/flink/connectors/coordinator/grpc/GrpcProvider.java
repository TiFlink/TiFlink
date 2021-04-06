package org.tikv.flink.connectors.coordinator.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
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
  private static final int MAX_PORT_NUM = 20000;
  private static final int MIN_PORT_NUM = 10000;

  private final GrpcService service;
  private final Server server;

  public GrpcProvider(final int port, final TiConfiguration conf) {
    this.service = new GrpcService(conf);
    this.server = ServerBuilder.forPort(port).addService(service).build();
    try {
      logger.info("starting server at: {}", port);
      server.start();
    } catch (final IOException e) {
      throw new RuntimeException("Unable to start server", e);
    }
  }

  public GrpcProvider(final TiConfiguration conf) {
    this(getRandomPort(), conf);
  }

  @Override
  public Coordinator createCoordinator() {
    return new GrpcClient(getServerAddresses());
  }

  @Override
  public void close() throws Exception {
    server.shutdown();
  }

  public List<InetSocketAddress> getServerAddresses() {
    return server.getListenSockets().stream()
        .filter(s -> s instanceof InetSocketAddress)
        .map(s -> (InetSocketAddress) s)
        .filter(s -> s.getAddress().isAnyLocalAddress())
        .collect(Collectors.toList());
  }

  private static int getRandomPort() {
    final Random rand = new Random();
    return Math.abs(rand.nextInt()) % (MAX_PORT_NUM - MIN_PORT_NUM) + MIN_PORT_NUM;
  }
}
