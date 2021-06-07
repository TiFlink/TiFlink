package org.tikv.flink.connectors.coordinator.grpc;

import java.net.InetSocketAddress;
import java.util.Map;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.tikv.flink.connectors.TiFlinkOptions;
import org.tikv.flink.connectors.coordinator.Coordinator;
import org.tikv.flink.connectors.coordinator.CoordinatorProvider;
import org.tikv.flink.connectors.coordinator.CoordinatorSupport;

public class GrpcProvider implements CoordinatorProvider {
  public static String IDENTIFIER = "grpc";

  public static String HOST_OPTION_KEY = "tiflink.coordinator.grpc.host";
  public static String PORT_OPTION_KEY = "tiflink.coordinator.grpc.port";

  public static ConfigOption<String> HOST_OPTION =
      ConfigOptions.key(HOST_OPTION_KEY)
          .stringType()
          .defaultValue("localhost")
          .withDescription("Host of the running GRPC server");

  public static ConfigOption<Integer> PORT_OPTION =
      ConfigOptions.key(PORT_OPTION_KEY)
          .intType()
          .defaultValue(54321)
          .withDescription("Host of the running GRPC server");

  @Override
  public String identifier() {
    return IDENTIFIER;
  }

  @Override
  public Coordinator createCoordinator(final Map<String, String> options) {
    final Configuration configuration = Configuration.fromMap(options);

    final String host = configuration.get(HOST_OPTION);
    final int port = configuration.get(PORT_OPTION);

    final InetSocketAddress address = InetSocketAddress.createUnresolved(host, port);
    return new GrpcClient(address);
  }

  @Override
  public CoordinatorSupport createSupport(final Map<String, String> options) {
    final Configuration configuration = Configuration.fromMap(options);

    final String host = configuration.get(HOST_OPTION);
    final int port = configuration.get(PORT_OPTION);

    return new GrpcServer(host, port, TiFlinkOptions.getTiConfiguration(options));
  }
}
