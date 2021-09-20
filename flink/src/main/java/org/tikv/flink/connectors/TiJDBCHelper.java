package org.tikv.flink.connectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Streams;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiJDBCHelper implements AutoCloseable {
  private static final Pattern VERSION_PATTERN = Pattern.compile("^(\\d+).(\\d+).(\\d+)(-.+)?$");

  private static final Logger LOGGER = LoggerFactory.getLogger(TiJDBCHelper.class);

  private static final String PD_KEY = "pd";
  private static final String TIDB_KEY = "tidb";
  private static final String TIKV_KEY = "tikv";

  private static final String QUERY_META_SQL =
      "SELECT `TYPE`, `INSTANCE`, `VERSION` FROM `INFORMATION_SCHEMA`.`CLUSTER_INFO`;";
  private static final String DROP_TABLE_SQL = "DROP TABLE IF EXISTS %s";
  private static final String CREATE_TABLE_SQL = "CREATE TABLE %s (%s, PRIMARY KEY(%s));";
  private static final String CREATE_TABLE_IF_NOT_EXISTS_SQL =
      "CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY(%s));";

  private final Connection connection;
  private final List<String> pdAddresses;
  private final Multimap<String, String> versions;
  private final String defaultDatabase;

  public TiJDBCHelper(final Connection connection) throws SQLException {
    this.connection = connection;
    try (final PreparedStatement stmt = connection.prepareStatement(QUERY_META_SQL)) {
      final ImmutableList.Builder<String> pdAddrBuilder = ImmutableList.builder();
      final ImmutableMultimap.Builder<String, String> versionsBuilder = ImmutableMultimap.builder();

      Preconditions.checkState(stmt.execute(), "Unable to query TiDB metadata");

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
      defaultDatabase = connection.getCatalog();

      Preconditions.checkState(!pdAddresses.isEmpty(), "No pd addresses found");
      Preconditions.checkState(
          checkVersions(versions.get(TIDB_KEY), "5.0.0"), "Unsupported TiDB version");
      Preconditions.checkState(
          checkVersions(versions.get(TIKV_KEY), "5.0.0"), "Unsupported TiKV version");
      Preconditions.checkState(
          checkVersions(versions.get(PD_KEY), "5.0.0"), "Unsupported PD version");

      LOGGER.info(
          "Initialized TiJDBCHelper, pdAddresses: {}, versions: {}, defaultDatabase: {}",
          pdAddresses,
          versions,
          defaultDatabase);
    }
  }

  public TiJDBCHelper(final String jdbcUrl, final Properties props) throws SQLException {
    this(DriverManager.getConnection(jdbcUrl, props));
  }

  public List<String> getPDAddresses() {
    return pdAddresses;
  }

  public Collection<String> getTidbVersions() {
    return versions.get(TIDB_KEY);
  }

  public Collection<String> getTikvVersions() {
    return versions.get(TIKV_KEY);
  }

  public Collection<String> getPdVersions() {
    return versions.get(PD_KEY);
  }

  public String getDefaultDatabase() {
    return defaultDatabase;
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
      final String pkDef =
          primaryKeys.stream().map(this::enquoteIdentifier).collect(Collectors.joining(", "));
      final String createTableSQL =
          String.format(
              ifNotExist ? CREATE_TABLE_IF_NOT_EXISTS_SQL : CREATE_TABLE_SQL,
              getQuotedPath(database, table),
              columnDefs,
              pkDef);
      LOGGER.info("create table with SQL: {}", createTableSQL);
      return stmt.execute(createTableSQL);
    }
  }

  public boolean dropTable(final String database, final String table) throws SQLException {
    try (final Statement stmt = connection.createStatement()) {
      final String dropTableSQL = String.format(DROP_TABLE_SQL, getQuotedPath(database, table));
      LOGGER.info("drop table with SQL: {}", dropTableSQL);
      return stmt.execute(dropTableSQL);
    }
  }

  public String getQuotedPath(final String... components) {
    return Stream.of(components).map(this::enquoteIdentifier).collect(Collectors.joining("."));
  }

  public String enquoteIdentifier(final String identifier) {
    // Assume MySQL quotation syntax
    return "`" + identifier.replaceAll("`", "``") + "`";
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }

  private boolean checkVersions(final Collection<String> versions, final String minVersion) {
    for (final String version : versions) {
      if (!checkVersion(version, minVersion)) {
        return false;
      }
    }
    return true;
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
    return enquoteIdentifier(name) + " " + dataType.toString();
  }
}
