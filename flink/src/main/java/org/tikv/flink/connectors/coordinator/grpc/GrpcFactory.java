package org.tikv.flink.connectors.coordinator.grpc;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import com.google.common.base.Preconditions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.tikv.common.TiConfiguration;
import org.tikv.flink.connectors.TiFlinkOptions;
import org.tikv.flink.connectors.coordinator.Coordinator;
import org.tikv.flink.connectors.coordinator.Factory;
import org.tikv.flink.connectors.coordinator.Provider;

public class GrpcFactory implements Factory {
  public static final String IDENTIFIER = "grpc";

  public static final String HOST_OPTION_KEY = "tiflink.coordinator.grpc.host";
  public static final String PORT_OPTION_KEY = "tiflink.coordinator.grpc.port";
  public static final String PORT_RANGE_OPTION_KEY = "tiflink.coordinator.grpc.port-range";

  public static final ConfigOption<String> HOST_OPTION =
      ConfigOptions.key(HOST_OPTION_KEY)
          .stringType()
          .defaultValue("localhost")
          .withDescription("Host of the running GRPC server");

  public static final ConfigOption<Integer> PORT_OPTION =
      ConfigOptions.key(PORT_OPTION_KEY)
          .intType()
          .noDefaultValue()
          .withDescription("Port of the GRPC server");

  public static final ConfigOption<String> PORT_RANGE_OPTION =
      ConfigOptions.key(PORT_RANGE_OPTION_KEY)
          .stringType()
          .defaultValue("30000-50000")
          .withDescription("Port range to random pick a GRPC port");

  @Override
  public String identifier() {
    return IDENTIFIER;
  }

  @Override
  public synchronized Provider createProvider(final Map<String, String> options) {
    final TiConfiguration tiConf = TiFlinkOptions.getTiConfiguration(options);
    final Configuration configuration = Configuration.fromMap(options);
    final String host = configuration.get(HOST_OPTION);
    final Optional<Integer> portOpt =
        configuration.getOptional(PORT_OPTION).or(() -> randomPortFromOptions(options));
    Preconditions.checkArgument(
        portOpt.isPresent(), "Either port or a valid port range must be specified");
    return new GrpcProvider(host, portOpt.get(), tiConf);
  }

  @Override
  public Coordinator createCoordinator(final Map<String, String> options) {
    final Configuration configuration = Configuration.fromMap(options);
    final String host = configuration.get(HOST_OPTION);
    final int port = configuration.get(PORT_OPTION);

    return new GrpcClient(host, port);
  }

  public static Optional<Integer> randomPortFromOptions(final Map<String, String> options) {
    final Configuration configuration = Configuration.fromMap(options);
    try {
      final String portRange = configuration.get(PORT_RANGE_OPTION);
      final String[] pair = portRange.split("-");
      final int min = Integer.parseInt(pair[0]);
      final int max = Integer.parseInt(pair[1]);
      final int port = ThreadLocalRandom.current().nextInt(min, max);
      return Optional.of(port);
    } catch (final Exception e) {
      return Optional.empty();
    }
  }
}
