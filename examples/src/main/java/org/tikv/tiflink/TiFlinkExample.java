package org.tikv.tiflink;

import com.google.common.base.Preconditions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.tikv.common.TiConfiguration;
import org.tikv.flink.connectors.TiFlinkCatalog;
import org.tikv.flink.connectors.coordinator.CoordinatorProvider;
import org.tikv.flink.connectors.coordinator.grpc.GrpcProvider;

public class TiFlinkExample {
  public static void main(final String[] args) {
    Preconditions.checkArgument(args.length == 1, "Must provide pdAddress");

    final String pdAddress = args[0];

    final String databaseName = "test";
    final String mvTable = "author_posts";

    final TiConfiguration conf = TiConfiguration.createDefault(pdAddress);
    final EnvironmentSettings settings =
        EnvironmentSettings.newInstance().inStreamingMode().build();

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(1000);
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    final CoordinatorProvider provider = getCoordinatorProvider(env, conf);
    final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
    tableEnv.registerCatalog("tikv", new TiFlinkCatalog(conf, "tikv", databaseName, provider));

    tableEnv.useCatalog("tikv");

    /* see examples/src/main/resources/example.sql for table schema and data */
    tableEnv
        .sqlQuery(
            "select id, first_name, last_name, email, "
                + "(select count(*) from posts where author_id = authors.id) as posts from authors")
        .executeInsert(mvTable);
  }

  public static CoordinatorProvider getCoordinatorProvider(
      final StreamExecutionEnvironment execEnv, final TiConfiguration conf) {
    if (execEnv instanceof RemoteStreamEnvironment) {
      final String host =
          ((RemoteStreamEnvironment) execEnv).getHost();
      return new GrpcProvider(host, 56789, conf);
    } else {
      return new GrpcProvider("localhost", 56789, conf);
    }
  }
}
