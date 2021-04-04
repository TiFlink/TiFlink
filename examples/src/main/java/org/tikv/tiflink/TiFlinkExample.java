package org.tikv.tiflink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.tikv.common.TiConfiguration;
import org.tikv.flink.connectors.TiFlinkCatalog;
import org.tikv.flink.connectors.coordinator.CoordinatorProvider;
import org.tikv.flink.connectors.coordinator.grpc.GrpcProvider;
import shade.com.google.common.base.Preconditions;

public class TiFlinkExample {
  public static void main(final String[] args) {
    Preconditions.checkArgument(args.length == 1, "Must provide pdAddress");

    final String pdAddress = args[0];

    final String databaseName = "test";
    final String mvTable = "author_posts";

    final TiConfiguration conf = TiConfiguration.createDefault(pdAddress);
    final EnvironmentSettings settings =
        EnvironmentSettings.newInstance().inStreamingMode().build();

    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
    env.setParallelism(1);
    env.enableCheckpointing(1000);
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
    env.getCheckpointConfig().setCheckpointTimeout(60000);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    final CoordinatorProvider provider = new GrpcProvider(conf);
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
}
