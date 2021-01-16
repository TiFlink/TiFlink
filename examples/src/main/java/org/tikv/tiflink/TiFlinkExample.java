package org.tikv.tiflink;

import java.util.Arrays;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.flink.connectors.TiFlinkCatalog;
import shade.com.google.common.base.Preconditions;

public class TiFlinkExample {
    public static void main(final String[] args) {
        Preconditions.checkArgument(args.length == 3, "Must provide pdAddress, databaseName and tableName");

        final String pdAddress = args[0];
        final String databaseName = args[1];
        final String tableName = args[2];

        final TiConfiguration conf = TiConfiguration.createDefault(pdAddress);
        final TiSession session = TiSession.create(conf);

        final EnvironmentSettings settings = EnvironmentSettings.newInstance()
            .inStreamingMode()
            .build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.registerCatalog("tikv", new TiFlinkCatalog(conf, "tikv", databaseName));

        tableEnv.useCatalog("tikv");
        final Table authors = tableEnv.from("authors");
        authors.execute().print();
    }    
}
