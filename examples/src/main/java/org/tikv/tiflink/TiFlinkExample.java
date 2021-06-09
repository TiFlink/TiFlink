package org.tikv.tiflink;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.tikv.common.TiConfiguration;
import org.tikv.flink.connectors.TiFlinkCatalog;
import org.tikv.flink.connectors.TiFlinkOptions;
import org.tikv.flink.connectors.coordinator.Provider;
import org.tikv.flink.connectors.coordinator.grpc.GrpcFactory;

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

    final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

    final Map<String, String> options =
        (env instanceof RemoteStreamEnvironment)
            ? Map.of(GrpcFactory.HOST_OPTION_KEY, ((RemoteStreamEnvironment) env).getHost())
            : Map.of();
    final Provider provider = TiFlinkOptions.getCoordinatorProvider(options);
    provider.start();

    tableEnv.registerCatalog("tikv", new TiFlinkCatalog(conf, "tikv", databaseName, provider.getCoordinatorOptions()));
    tableEnv.useCatalog("tikv");
    // final Parser parser = ((TableEnvironmentImpl) tableEnv).getParser();
    // final List<Operation> operations =
    //     parser.parse(
    //         "select id, first_name, last_name, email, (select count(*) from posts where
    // author_id"
    //             + " = authors.id) as posts from authors");

    // final QueryOperation operation = (QueryOperation) operations.get(0);

    // System.out.println(operation.getResolvedSchema());
    //

    /* see examples/src/main/resources/example.sql for table schema and data */
    tableEnv
        .sqlQuery(
            "select id, first_name, last_name, email, "
                + "(select count(*) from posts where author_id = authors.id) as posts from authors")
        .executeInsert(mvTable);
  }
}
