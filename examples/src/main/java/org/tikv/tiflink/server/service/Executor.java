package org.tikv.tiflink.server.service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.tikv.common.TiConfiguration;
import org.tikv.flink.connectors.TiFlinkCatalog;

@Service
public class Executor {

  @Value("${tidb.url}")
  private String url;

  @Value("${tidb.username}")
  private String username;

  @Value("${tidb.password}")
  private String password;

  @Value("${pd.url}")
  private String pdUrl;

  @Value("database")
  private String databaseName;

  TiConfiguration conf;

  private StreamTableEnvironment tableEnv;

  public String execute(String sql, String insertTableName) throws Exception {
    if (conf == null) {
      synchronized (this) {
        if (conf == null) {
          conf = TiConfiguration.createDefault(pdUrl);
          EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

          final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.enableCheckpointing(1000);
          env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
          env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
          env.getCheckpointConfig().setCheckpointTimeout(60000);
          env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
          tableEnv = StreamTableEnvironment.create(env, settings);
          tableEnv.registerCatalog("tikv", new TiFlinkCatalog(conf, "tikv", databaseName));

          tableEnv.useCatalog("tikv");
        }
      }
    }
    return tableEnv.sqlQuery(sql).executeInsert(insertTableName).toString();
  }

  private Map<String, String> getProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("tidb.database.url", url);
    properties.put("tidb.username", username);
    properties.put("tidb.password", password);
    return properties;
  }

  private AtomicBoolean imported = new AtomicBoolean(true);
}
