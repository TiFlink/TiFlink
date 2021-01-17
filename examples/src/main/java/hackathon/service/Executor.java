package hackathon.service;

import com.zhihu.tibigdata.flink.tidb.TiDBCatalog;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.tikv.common.TiConfiguration;
import org.tikv.flink.connectors.TiFlinkCatalog;
import tidb.hackathon.entity.Sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

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

  public String execute(Sql sql) throws Exception {
    if (StringUtils.isNotEmpty(sql.getDdl())) {
      if (StringUtils.isEmpty(sql.getCatalog())) {
        tableEnv.executeSql(sql.getDdl());
      } else {
        TiDBCatalog catalog = new TiDBCatalog(getProperties());
        catalog.open();
        catalog.sqlUpdate(sql.getDdl());
        catalog.close();
      }
    } else if (StringUtils.isNotEmpty(sql.getDml())) {
      if (StringUtils.isEmpty(sql.getCatalog())) {
        tableEnv.execute(sql.getDml());
      } else {
        TiDBCatalog catalog = new TiDBCatalog(getProperties());
        catalog.open();
        tableEnv.registerCatalog("tidb", catalog);
        tableEnv.useCatalog("tidb");
        tableEnv.executeSql(sql.getDml());
      }
    } else if (StringUtils.isNotEmpty(sql.getDql())) {
      TableResult tableResult;
      if (!StringUtils.isEmpty(sql.getCatalog())) {
        TiDBCatalog catalog = new TiDBCatalog(getProperties());
        catalog.open();
        tableEnv.registerCatalog("tidb", catalog);
        tableEnv.useCatalog("tidb");
      }
      tableResult = tableEnv.executeSql(sql.getDql());
      List<String> tableResults = new ArrayList<>();
      try (CloseableIterator<Row> iterator = tableResult.collect()) {
        while (iterator.hasNext()) {
          Row row = iterator.next();
          tableResults.add(row.toString());
        }
      }
      return String.join("\n", tableResults);
    }
    return "";
  }

  private Map<String, String> getProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("tidb.database.url", url);
    properties.put("tidb.username", username);
    properties.put("tidb.password", password);
    return properties;
  }

  private AtomicBoolean imported = new AtomicBoolean(true);
//  public void makeSureInit() throws Exception {
//    if(imported.compareAndSet(false, true)) {
//      Sql sql = new Sql();
//      sql.setDdl("\n" +
//              "create table wide_stuff(\n" +
//              "    stuff_id int primary key,\n" +
//              "    base_id int,\n" +
//              "    base_location varchar(20),\n" +
//              "    stuff_name varchar(20)\n" +
//              ") WITH (\n" +
//              "\t'connector'  = 'jdbc',\n" +
//              "    'driver'     = 'com.mysql.cj.jdbc.Driver',\n" +
//              "    'url'        = 'jdbc:mysql://tidb:4000/test?rewriteBatchedStatements=true',\n" +
//              "    'table-name' = 'wide_stuff',\n" +
//              "    'username'   = 'root',\n" +
//              "    'password'   = ''\n" +
//              ");");
//      execute(sql);
//
//      Sql sql1 = new Sql();
//      sql.setDdl("create table stuff(\n" +
//              "    stuff_id int primary key,\n" +
//              "    stuff_base_id int,\n" +
//              "    stuff_name varchar(20)\n" +
//              ") WITH (\n" +
//              "    'connector' = 'mysql-cdc',\n" +
//              "    'hostname' = 'mysql',\n" +
//              "    'port' = '3306',\n" +
//              "    'username' = 'root',\n" +
//              "    'password' = '',\n" +
//              "    'database-name' = 'test',\n" +
//              "    'table-name' = 'stuff'\n" +
//              "); ");
//      execute(sql1);
//    }
//  }
}
