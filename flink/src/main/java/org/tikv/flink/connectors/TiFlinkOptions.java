package org.tikv.flink.connectors;

import java.util.Map;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.tikv.common.ConfigUtils;
import org.tikv.common.TiConfiguration;
import org.tikv.flink.connectors.coordinator.Coordinator;
import org.tikv.flink.connectors.coordinator.CoordinatorProvider;
import org.tikv.flink.connectors.coordinator.CoordinatorSupport;
import org.tikv.flink.connectors.coordinator.grpc.GrpcProvider;

public class TiFlinkOptions {
  public static String COORDINATOR_PROVIDER_KEY = "tiflink.coordinator.provider";

  private TiFlinkOptions() {}

  public static ConfigOption<String> TIKV_PD_ADDRESSES =
      ConfigOptions.key(ConfigUtils.TIKV_PD_ADDRESSES)
          .stringType()
          .noDefaultValue()
          .withDescription("TiKV cluster's PD address");

  public static ConfigOption<Long> TIKV_GRPC_TIMEOUT =
      ConfigOptions.key(ConfigUtils.TIKV_GRPC_TIMEOUT)
          .longType()
          .noDefaultValue()
          .withDescription("TiKV GRPC timeout in ms");

  public static ConfigOption<Long> TIKV_GRPC_SCAN_TIMEOUT =
      ConfigOptions.key(ConfigUtils.TIKV_GRPC_SCAN_TIMEOUT)
          .longType()
          .noDefaultValue()
          .withDescription("TiKV GRPC scan timeout in ms");

  public static ConfigOption<Integer> TIKV_BATCH_GET_CONCURRENCY =
      ConfigOptions.key(ConfigUtils.TIKV_BATCH_GET_CONCURRENCY)
          .intType()
          .noDefaultValue()
          .withDescription("TiKV GRPC batch get concurrency");

  public static ConfigOption<Integer> TIKV_BATCH_PUT_CONCURRENCY =
      ConfigOptions.key(ConfigUtils.TIKV_BATCH_PUT_CONCURRENCY)
          .intType()
          .noDefaultValue()
          .withDescription("TiKV GRPC batch put concurrency");

  public static ConfigOption<Integer> TIKV_BATCH_SCAN_CONCURRENCY =
      ConfigOptions.key(ConfigUtils.TIKV_BATCH_SCAN_CONCURRENCY)
          .intType()
          .noDefaultValue()
          .withDescription("TiKV GRPC batch scan concurrency");

  public static ConfigOption<Integer> TIKV_BATCH_DELETE_CONCURRENCY =
      ConfigOptions.key(ConfigUtils.TIKV_BATCH_DELETE_CONCURRENCY)
          .intType()
          .noDefaultValue()
          .withDescription("TiKV GRPC batch delete concurrency");

  public static ConfigOption<String> COORDINATOR_PROVIDER =
      ConfigOptions.key(COORDINATOR_PROVIDER_KEY)
          .stringType()
          .noDefaultValue()
          .withDescription("Coordinator provider name");

  public static TiConfiguration getTiConfiguration(final Map<String, String> options) {
    final Configuration configuration = Configuration.fromMap(options);

    final TiConfiguration tiConf =
        configuration
            .getOptional(TIKV_PD_ADDRESSES)
            .map(TiConfiguration::createDefault)
            .orElseGet(TiConfiguration::createDefault);

    configuration.getOptional(TIKV_GRPC_TIMEOUT).ifPresent(tiConf::setTimeout);
    configuration.getOptional(TIKV_GRPC_SCAN_TIMEOUT).ifPresent(tiConf::setScanTimeout);
    configuration.getOptional(TIKV_BATCH_GET_CONCURRENCY).ifPresent(tiConf::setBatchGetConcurrency);
    configuration.getOptional(TIKV_BATCH_PUT_CONCURRENCY).ifPresent(tiConf::setBatchPutConcurrency);
    configuration
        .getOptional(TIKV_BATCH_SCAN_CONCURRENCY)
        .ifPresent(tiConf::setBatchScanConcurrency);
    configuration
        .getOptional(TIKV_BATCH_DELETE_CONCURRENCY)
        .ifPresent(tiConf::setBatchDeleteConcurrency);
    return tiConf;
  }

  public static CoordinatorProvider getCoordinatorProvider(final Map<String, String> options) {
    final Configuration configuration = Configuration.fromMap(options);
    final String providerName =
        configuration.getOptional(COORDINATOR_PROVIDER).orElse(GrpcProvider.IDENTIFIER);
    return CoordinatorProvider.get(providerName);
  }

  public static Coordinator getCoordinator(final Map<String, String> options) {
    return getCoordinatorProvider(options).createCoordinator(options);
  }

  public static CoordinatorSupport getCoordinatorSupport(final Map<String, String> options) {
    return getCoordinatorProvider(options).createSupport(options);
  }
}
