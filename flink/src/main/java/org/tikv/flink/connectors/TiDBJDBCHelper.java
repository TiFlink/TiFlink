package org.tikv.flink.connectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.DataType;

public class TiDBJDBCHelper {
  private static final Pattern VERSION_PATTERN = Pattern.compile("^(\\d+).(\\d+).(\\d+)(-.+)?$");

  private static final String PD_KEY = "pd";
  private static final String TIDB_KEY = "tidb";
  private static final String TIKV_KEY = "tikv";

  private static final String QUERY_META_SQL =
      "SELECT `TYPE`, `INSTANCE`, `VERSION` FROM `INFORMATION_SCHEMA`.`CLUSTER_INFO`;";
  private static final String DROP_TABLE_SQL = "DROP TABLE IF EXISTS %s.%s";
  private static final String CREATE_TABLE_SQL = "DROP TABLE %s.%s (%s, PRIMARY KEY(%s));";
  private static final String CREATE_TABLE_IF_NOT_EXISTS_SQL =
      "DROP TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY(%s));";

  private final Connection connection;
  private final List<String> pdAddresses;
  private final Map<String, String> versions;

  public TiDBJDBCHelper(final Connection connection) throws SQLException {
    this.connection = connection;
    try (final Statement stmt = connection.prepareStatement(QUERY_META_SQL)) {
      final ImmutableList.Builder<String> pdAddrBuilder = ImmutableList.builder();
      final ImmutableMap.Builder<String, String> versionsBuilder = ImmutableMap.builder();
      final ResultSet res = stmt.getResultSet();

      while (res.next()) {
        final String type = res.getString(1);
        switch (type) {
          case PD_KEY:
            pdAddrBuilder.add(res.getString(2));
          default:
            versionsBuilder.put(type, res.getString(3));
        }
      }

      pdAddresses = pdAddrBuilder.build();
      versions = versionsBuilder.build();

      Preconditions.checkState(!pdAddresses.isEmpty(), "No pd addresses found");
      Preconditions.checkState(
          checkVersion(versions.get(TIDB_KEY), "5.0.0"), "Unsupported TiDB version");
      Preconditions.checkState(
          checkVersion(versions.get(TIKV_KEY), "5.0.0"), "Unsupported TiKV version");
      Preconditions.checkState(
          checkVersion(versions.get(PD_KEY), "5.0.0"), "Unsupported PD version");
    }
  }

  public TiDBJDBCHelper(final String jdbcUrl, final Properties props) throws SQLException {
    this(DriverManager.getConnection(jdbcUrl, props));
  }

  public List<String> getPDAddresses() {
    return pdAddresses;
  }

  public String getTiDBVersion() {
    return versions.get(TIDB_KEY);
  }

  public String getTiKVVersion() {
    return versions.get(TIKV_KEY);
  }

  public String getPDVersion() {
    return versions.get(PD_KEY);
  }

  public boolean createTable(
      final String database,
      final String table,
      final List<String> columnNames,
      final List<DataType> columnTypes,
      final List<String> primaryKeys,
      final boolean ifNotExist)
      throws SQLException {
    Preconditions.checkArgument(
        columnTypes.size() == columnTypes.size(), "Mismatched columnNames and columnTypes");
    Preconditions.checkArgument(
        Set.copyOf(columnNames).containsAll(primaryKeys),
        "PrimaryKeys must be included by columnNames");
    Preconditions.checkArgument(!primaryKeys.isEmpty(), "Empty primaryKeys");
    try (final Statement stmt = connection.createStatement()) {
      final String columnDefs =
          Streams.zip(columnNames.stream(), columnTypes.stream(), this::getColumnDef)
              .collect(Collectors.joining(", "));
      final String pkDef = primaryKeys.stream().collect(Collectors.joining(", "));
      return stmt.execute(
          String.format(
              ifNotExist ? CREATE_TABLE_IF_NOT_EXISTS_SQL : CREATE_TABLE_SQL,
              stmt.enquoteIdentifier(database, true),
              stmt.enquoteIdentifier(table, true),
              columnDefs,
              pkDef));
    }
  }

  public boolean createTable(
      final String database,
      final String table,
      final ResolvedSchema schema,
      final boolean ifNotExist)
      throws SQLException {
    final List<String> columnNames = schema.getColumnNames();
    final List<DataType> columnTypes = schema.getColumnDataTypes();
    final List<String> primaryKeys =
        schema.getPrimaryKey().map(UniqueConstraint::getColumns).orElse(List.of());
    return createTable(database, table, columnNames, columnTypes, primaryKeys, ifNotExist);
  }

  public boolean dropTable(final String database, final String table) throws SQLException {
    try (final Statement stmt = connection.createStatement()) {
      return stmt.execute(
          String.format(
              DROP_TABLE_SQL,
              stmt.enquoteIdentifier(database, true),
              stmt.enquoteIdentifier(table, true)));
    }
  }

  private boolean checkVersion(final String version, final String minVersion) {
    Preconditions.checkNotNull(version);
    Preconditions.checkNotNull(minVersion);

    final Matcher versionMatcher = VERSION_PATTERN.matcher(version);
    final Matcher minVersionMatcher = VERSION_PATTERN.matcher(minVersion);

    Preconditions.checkArgument(versionMatcher.matches(), "Illegal version value");
    Preconditions.checkArgument(minVersionMatcher.matches(), "Illegal minVersion value");

    for (int i = 1; i < 3; i++) {
      if (Integer.parseInt(versionMatcher.group(i))
          < Integer.parseInt(minVersionMatcher.group(i))) {
        return false;
      }
    }

    return true;
  }

  private String getColumnDef(final String name, final DataType dataType) {
    return name + " " + dataType.toString();
  }
}
