package org.tikv.tiflink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.tikv.common.TiConfiguration;
import org.tikv.flink.connectors.TiFlinkCatalog;
import shade.com.google.common.base.Preconditions;

public class TiFlinkExample {
    public static void main(final String[] args) {
        Preconditions.checkArgument(args.length == 1, "Must provide pdAddress");

        final String pdAddress = args[0];

        final String databaseName = "test";
        final String tableName = "authors";
        final String targetTableName = "authors_mv";

        final TiConfiguration conf = TiConfiguration.createDefault(pdAddress);
        final EnvironmentSettings settings = EnvironmentSettings.newInstance()
            .inStreamingMode()
            .build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.registerCatalog("tikv", new TiFlinkCatalog(conf, "tikv", databaseName));

        tableEnv.useCatalog("tikv");
        final Table authors = tableEnv.from(tableName);

        authors.executeInsert(targetTableName);
    }    
}
