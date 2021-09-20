package org.tikv.flink.connectors;

import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.DataType;
import org.tikv.common.TiConfiguration;
import org.tikv.flink.connectors.coordinator.Provider;
import org.tikv.flink.connectors.coordinator.grpc.GrpcFactory;

public class TiFlinkApp implements AutoCloseable {
  private static final String CATALOG_NAME = "tiflink";

  private final TiJDBCHelper jdbcHelper;
  private final Provider coordinatorProvider;
  private final StreamTableEnvironment tableEnv;
  private final Table mvTable;

  private final String defaultDatabase;
  private final String targetDatabase;
  private final String targetTable;
  private final List<String> columnNames;
  private final List<String> primaryKeys;
  private final boolean dropOldTable;
  private final boolean forceNewTable;

  protected TiFlinkApp(
      final TiJDBCHelper jdbcHelper,
      final Provider coordinatorProvider,
      final StreamTableEnvironment tableEnv,
      final Table mvTable,
      final String defaultDatabase,
      final String targetDatabase,
      final String targetTable,
      final List<String> columnNames,
      final List<String> primaryKeys,
      final boolean dropOldTable,
      final boolean forceNewTable) {
    this.jdbcHelper = jdbcHelper;
    this.coordinatorProvider = coordinatorProvider;
    this.tableEnv = tableEnv;
    this.mvTable = mvTable;
    this.defaultDatabase = defaultDatabase;
    this.targetDatabase = targetDatabase;
    this.targetTable = targetTable;
    this.columnNames = columnNames;
    this.primaryKeys = primaryKeys;
    this.dropOldTable = dropOldTable;
    this.forceNewTable = forceNewTable;
  }

  public void start() throws Exception {
    tableEnv.useCatalog(CATALOG_NAME);
    tableEnv.useDatabase(defaultDatabase);
    try {
      ensureTargetTable();

      coordinatorProvider.start();
      mvTable.executeInsert(jdbcHelper.getQuotedPath(targetDatabase, targetTable));
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      close();
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  protected void ensureTargetTable() {
    try {
      if (dropOldTable) {
        jdbcHelper.dropTable(targetDatabase, targetTable);
      }
      final List<DataType> columnTypes = mvTable.getResolvedSchema().getColumnDataTypes();
      jdbcHelper.createTable(
          targetDatabase, targetTable, columnNames, columnTypes, primaryKeys, !forceNewTable);
    } catch (final SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    coordinatorProvider.close();
    jdbcHelper.close();
  }

  public static class Builder {
    private Connection jdbcConnection = null;
    private Provider coordinatorProvider = null;
    private String defaultDatabase = null;
    private String targetDatabase = null;
    private String targetTable = null;
    private String query = null;
    private EnvironmentSettings envSettings = null;
    private List<String> columnNames = null;
    private List<String> primaryKeys = null;

    private int parallism = 1;
    private int checkpointInterval = 1000;
    private boolean dropOldTable = false;
    private boolean forceNewTable = true;

    private Builder() {}

    public Builder setJdbcUrl(final String jdbcUrl) throws SQLException {
      return setJdbcUrl(jdbcUrl, new Properties());
    }

    public Builder setJdbcUrl(final String jdbcUrl, final Properties props) throws SQLException {
      return setJdbcConnection(DriverManager.getConnection(jdbcUrl, props));
    }

    public Builder setJdbcConnection(final Connection conn) {
      this.jdbcConnection = conn;
      return this;
    }

    public Builder setCoordinatorProvider(final Provider provider) {
      this.coordinatorProvider = provider;
      return this;
    }

    public Builder setDefaultDatabase(final String defaultDatabase) {
      this.defaultDatabase = defaultDatabase;
      return this;
    }

    public Builder setTargetTable(final String table) {
      this.targetTable = table;
      return this;
    }

    public Builder setTargetTable(final String database, final String table) {
      this.targetDatabase = database;
      this.targetTable = table;
      return this;
    }

    public Builder setQuery(final String query) {
      this.query = query;
      return this;
    }

    public Builder setEnvironmentSettings(final EnvironmentSettings settings) {
      this.envSettings = settings;
      return this;
    }

    public Builder setColumnNames(final String... colNames) {
      return this.setColumnNames(Arrays.asList(colNames));
    }

    public Builder setColumnNames(final List<String> colNames) {
      this.columnNames = colNames;
      return this;
    }

    public Builder setPrimaryKeys(final String... pkNames) {
      return setPrimaryKeys(Arrays.asList(pkNames));
    }

    public Builder setPrimaryKeys(final List<String> pkNames) {
      this.primaryKeys = pkNames;
      return this;
    }

    public Builder setParallelism(final int parallism) {
      this.parallism = parallism;
      return this;
    }

    public Builder setCheckpointInterval(final int interval) {
      this.checkpointInterval = interval;
      return this;
    }

    public Builder setDropOldTable(final boolean dropOldTable) {
      this.dropOldTable = dropOldTable;
      return this;
    }

    public Builder setForceNewTable(final boolean forceNewTable) {
      this.forceNewTable = forceNewTable;
      return this;
    }

    public TiFlinkApp build() throws SQLException {
      Preconditions.checkNotNull(jdbcConnection, "JDBC Connection must be specified");

      final TiJDBCHelper helper = new TiJDBCHelper(jdbcConnection);
      defaultDatabase = defaultDatabase == null ? helper.getDefaultDatabase() : defaultDatabase;
      Preconditions.checkNotNull(
          defaultDatabase,
          "Default database must be specified either by jdbc connection or parameter");
      Preconditions.checkState(
          helper.getDefaultDatabase() == null
              || defaultDatabase.equals(helper.getDefaultDatabase()),
          "Default database mismatched");
      targetDatabase = targetDatabase == null ? defaultDatabase : targetDatabase;

      final TiConfiguration tiConf = TiConfiguration.createDefault(helper.getPDAddresses().get(0));

      final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(parallism);
      env.enableCheckpointing(checkpointInterval);
      env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
      env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
      env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

      envSettings =
          envSettings == null
              ? EnvironmentSettings.newInstance().inStreamingMode().build()
              : envSettings;
      Preconditions.checkArgument(
          envSettings.isStreamingMode(), "EnvironmentSettings must be in streaming mode");

      if (coordinatorProvider == null) {
        final Map<String, String> options =
            (env instanceof RemoteStreamEnvironment)
                ? Map.of(GrpcFactory.HOST_OPTION_KEY, ((RemoteStreamEnvironment) env).getHost())
                : Map.of();
        coordinatorProvider = TiFlinkOptions.getCoordinatorProvider(options);
      }

      final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, envSettings);
      tableEnv.registerCatalog(
          CATALOG_NAME,
          new TiFlinkCatalog(
              tiConf, CATALOG_NAME, defaultDatabase, coordinatorProvider.getCoordinatorOptions()));
      tableEnv.useCatalog(CATALOG_NAME);

      final Table mvTable = tableEnv.sqlQuery(query);
      final List<String> queryColNames = mvTable.getResolvedSchema().getColumnNames();
      columnNames = columnNames == null ? queryColNames : columnNames;
      Preconditions.checkState(
          columnNames.size() == queryColNames.size(), "Mismatched size of columnNames");

      final List<String> queryPrimaryKeys =
          mvTable
              .getResolvedSchema()
              .getPrimaryKey()
              .map(UniqueConstraint::getColumns)
              .orElse(Arrays.asList(columnNames.get(0)));

      primaryKeys = primaryKeys == null ? queryPrimaryKeys : primaryKeys;
      Preconditions.checkState(!primaryKeys.isEmpty(), "PrimaryKeys can't be empty");
      Preconditions.checkState(
          Set.copyOf(columnNames).containsAll(primaryKeys),
          "PrimaryKeys must be contained by columnNames");

      return new TiFlinkApp(
          helper,
          coordinatorProvider,
          tableEnv,
          mvTable,
          defaultDatabase,
          targetDatabase,
          targetTable,
          columnNames,
          primaryKeys,
          dropOldTable,
          forceNewTable);
    }
  }
}
