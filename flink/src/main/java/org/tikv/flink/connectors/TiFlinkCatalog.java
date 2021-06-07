package org.tikv.flink.connectors;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiDBInfo;
import org.tikv.common.meta.TiIndexColumn;
import org.tikv.common.meta.TiIndexInfo;
import org.tikv.common.meta.TiPartitionInfo;
import org.tikv.common.meta.TiTableInfo;

public class TiFlinkCatalog implements Catalog {

  private final TiConfiguration conf;
  private final String name;
  private final String defaultDatabase;
  private final Map<String, String> tableOptions;

  private Optional<TiSession> session = Optional.empty();

  public TiFlinkCatalog(
      final TiConfiguration conf,
      final String name,
      final String defaultDatabase,
      final Map<String, String> defaultOptions) {
    this.conf = conf;
    this.name = name;
    this.defaultDatabase = defaultDatabase;
    this.tableOptions =
        ImmutableMap.<String, String>builder()
            .putAll(defaultOptions)
            .put("connector", "tiflink")
            .build();
  }

  @Override
  public void open() throws CatalogException {
    session = Optional.ofNullable(TiSession.create(conf));
  }

  @Override
  public void close() throws CatalogException {
    try {
      if (session.isPresent()) {
        session.get().close();
      }
    } catch (final Exception e) {
      throw new CatalogException(e);
    }
  }

  @Override
  public String getDefaultDatabase() throws CatalogException {
    return defaultDatabase;
  }

  @Override
  public List<String> listDatabases() throws CatalogException {
    return session.stream()
        .flatMap(s -> s.getCatalog().listDatabases().stream())
        .map(TiDBInfo::getName)
        .collect(Collectors.toList());
  }

  @Override
  public CatalogDatabase getDatabase(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    Optional<CatalogDatabase> res =
        session
            .flatMap(s -> Optional.ofNullable(s.getCatalog().getDatabase(databaseName)))
            .map(db -> new CatalogDatabaseImpl(Collections.emptyMap(), ""));

    if (res.isEmpty()) {
      throw new DatabaseNotExistException(name, databaseName);
    }

    return res.get();
  }

  @Override
  public boolean databaseExists(String databaseName) throws CatalogException {
    return session
        .flatMap(s -> Optional.ofNullable(s.getCatalog().getDatabase(databaseName)))
        .isPresent();
  }

  @Override
  public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
      throws DatabaseAlreadyExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
      throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
      throws DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listTables(final String databaseName)
      throws DatabaseNotExistException, CatalogException {
    final Optional<TiDBInfo> db =
        session.flatMap(s -> Optional.ofNullable(s.getCatalog().getDatabase(databaseName)));
    if (db.isEmpty()) {
      throw new DatabaseNotExistException(name, databaseName);
    }

    return session.stream()
        .flatMap(s -> s.getCatalog().listTables(db.get()).stream())
        .filter(Predicate.not(TiTableInfo::isView))
        .map(TiTableInfo::getName)
        .collect(Collectors.toList());
  }

  @Override
  public List<String> listViews(final String databaseName)
      throws DatabaseNotExistException, CatalogException {
    final Optional<TiDBInfo> db =
        session.flatMap(s -> Optional.ofNullable(s.getCatalog().getDatabase(databaseName)));
    if (db.isEmpty()) {
      throw new DatabaseNotExistException(name, databaseName);
    }

    return session.stream()
        .flatMap(s -> s.getCatalog().listTables(db.get()).stream())
        .filter(TiTableInfo::isView)
        .map(TiTableInfo::getName)
        .collect(Collectors.toList());
  }

  @Override
  public CatalogBaseTable getTable(final ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    Optional<CatalogTable> res =
        session
            .flatMap(
                s ->
                    Optional.ofNullable(
                        s.getCatalog()
                            .getTable(tablePath.getDatabaseName(), tablePath.getObjectName())))
            .map(this::getCatalogTable);
    if (res.isEmpty()) {
      throw new TableNotExistException("name", tablePath);
    }
    return res.get();
  }

  @Override
  public boolean tableExists(final ObjectPath tablePath) throws CatalogException {
    return session
        .flatMap(s -> Optional.ofNullable(s.getCatalog().getDatabase(tablePath.getDatabaseName())))
        .flatMap(
            db ->
                db.getTables().stream()
                    .filter(t -> t.getName().equals(tablePath.getObjectName()))
                    .findFirst())
        .isPresent();
  }

  @Override
  public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
      throws TableNotExistException, TableAlreadyExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
      throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
          CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitionsByFilter(
      ObjectPath tablePath, List<Expression> filters)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws CatalogException {
    return false;
  }

  @Override
  public void createPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition partition,
      boolean ignoreIfExists)
      throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
          PartitionAlreadyExistsException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropPartition(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition newPartition,
      boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listFunctions(final String dbName)
      throws DatabaseNotExistException, CatalogException {
    return Collections.emptyList();
  }

  @Override
  public CatalogFunction getFunction(ObjectPath functionPath)
      throws FunctionNotExistException, CatalogException {
    throw new FunctionNotExistException(name, functionPath);
  }

  @Override
  public boolean functionExists(ObjectPath functionPath) throws CatalogException {
    return false;
  }

  @Override
  public void createFunction(
      ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
      throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterFunction(
      ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
      throws FunctionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
      throws FunctionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogTableStatistics getTableStatistics(final ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    return null;
  }

  @Override
  public CatalogColumnStatistics getTableColumnStatistics(final ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    return null;
  }

  @Override
  public CatalogTableStatistics getPartitionStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    return null;
  }

  @Override
  public CatalogColumnStatistics getPartitionColumnStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    return null;
  }

  @Override
  public void alterTableStatistics(
      ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTableColumnStatistics(
      ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException, TablePartitionedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartitionStatistics(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogTableStatistics partitionStatistics,
      boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartitionColumnStatistics(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogColumnStatistics columnStatistics,
      boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  public Schema getTableSchema(final TiTableInfo tableInfo) {
    final Schema.Builder builder = Schema.newBuilder();
    tableInfo
        .getColumns()
        .forEach(col -> builder.column(col.getName(), TypeUtils.getFlinkType(col.getType())));
    final Optional<TiIndexInfo> pkIndexOptional =
        tableInfo.getIndices().stream().filter(TiIndexInfo::isPrimary).findFirst();
    if (pkIndexOptional.isPresent()) {
      final TiIndexInfo pkIndexInfo = pkIndexOptional.get();
      builder.primaryKey(
          pkIndexInfo.getIndexColumns().stream()
              .map(TiIndexColumn::getName)
              .toArray(String[]::new));
    } else {
      builder.primaryKey(
          tableInfo.getColumns().stream()
              .filter(TiColumnInfo::isPrimaryKey)
              .map(TiColumnInfo::getName)
              .toArray(String[]::new));
    }
    return builder.build();
  }

  public CatalogTable getCatalogTable(final TiTableInfo tableInfo) {
    final String comment = Objects.toString(tableInfo.getComment());
    final TiPartitionInfo partitionInfo = tableInfo.getPartitionInfo();
    final List<String> partitions =
        Objects.isNull(partitionInfo) ? List.of() : partitionInfo.getColumns();
    return CatalogTable.of(getTableSchema(tableInfo), comment, partitions, tableOptions);
  }
}
