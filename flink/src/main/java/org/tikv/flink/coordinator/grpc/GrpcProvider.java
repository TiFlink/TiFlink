package org.tikv.flink.coordinator.grpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.flink.TiFlinkOptions;
import org.tikv.flink.coordinator.Provider;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

public class GrpcProvider implements Provider {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcProvider.class);

  private final InetSocketAddress address;
  private final GrpcService service;
  private final Server server;

  public GrpcProvider(final String host, final int port, final TiConfiguration conf) {
    address = new InetSocketAddress(host, port);
    service = new GrpcService(conf);
    server = NettyServerBuilder.forAddress(address).addService(service).build();
  }

  @Override
  public Map<String, String> getCoordinatorOptions() {
    return Map.of(
        TiFlinkOptions.COORDINATOR_PROVIDER_KEY, GrpcFactory.IDENTIFIER,
        GrpcFactory.HOST_OPTION_KEY, address.getHostName(),
        GrpcFactory.PORT_OPTION_KEY, String.valueOf(address.getPort()));
  }

  public void start() {
    try {
      LOGGER.info("Start GRPC server at: {}", address);
      server.start();
    } catch (final IOException e) {
      LOGGER.error("Failed to start GRPC server", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("Stopping GRPC server");
    server.shutdown();
  }
}
