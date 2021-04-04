package org.tikv.flink.connectors.coordinator.grpc;

import com.google.common.base.Throwables;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import org.tikv.common.TiConfiguration;
import org.tikv.flink.connectors.coordinator.Coordinator;
import org.tikv.flink.connectors.coordinator.CoordinatorProvider;

public class GrpcProvider implements CoordinatorProvider, AutoCloseable {
  private static final String DEFAULT_BIND_ADDR = "0.0.0.0";
  private static final int MAX_PORT_NUM = 20000;
  private static final int MIN_PORT_NUM = 10000;

  private final URI serverURI;
  private final GrpcService service;
  private final Server server;

  public GrpcProvider(final URI serverURI, final TiConfiguration conf) {
    this.serverURI = serverURI;
    this.service = new GrpcService(conf);
    this.server = ServerBuilder.forPort(serverURI.getPort()).addService(service).build();
  }

  public GrpcProvider(final TiConfiguration conf) {
    this(getRandomURI(), conf);
  }

  @Override
  public Coordinator createCoordinator() {
    return new GrpcClient(serverURI);
  }

  @Override
  public void close() throws Exception {
    server.shutdown();
  }

  @Override
  public void start() {
    try {
      server.start();
    } catch (final IOException e) {
      Throwables.throwIfUnchecked(e);
    }
  }

  private static URI getRandomURI() {
    final Random rand = new Random();
    final int port = rand.nextInt() % (MAX_PORT_NUM - MIN_PORT_NUM) + MIN_PORT_NUM;
    try {
      return new URI(DEFAULT_BIND_ADDR + ":" + port);
    } catch (final URISyntaxException e) {
      // should never throw
      Throwables.throwIfUnchecked(e);
    }
    return null;
  }
}
