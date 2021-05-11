package org.tikv.flink.connectors;

import java.util.Objects;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.types.RowKind;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.flink.connectors.coordinator.Coordinator;

public class TikvDynamicSource implements ScanTableSource {

  private final String pdAddress;
  private final String database;
  private final String table;
  private final Coordinator coordinator;

  public TikvDynamicSource(
      final String pdAddress,
      final String database,
      final String table,
      final Coordinator coordinator) {
    this.pdAddress = pdAddress;
    this.database = database;
    this.table = table;
    this.coordinator = coordinator;
  }

  @Override
  public DynamicTableSource copy() {
    return new TikvDynamicSource(pdAddress, database, table, coordinator);
  }

  @Override
  public String asSummaryString() {
    return String.format("TiKV Table[`%s`.`%s`]", database, table);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.newBuilder()
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.DELETE)
        .build();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(final ScanContext runtimeProviderContext) {
    final TiConfiguration conf = TiConfiguration.createDefault(pdAddress);
    try (final TiSession session = TiSession.create(conf)) {
      final TiTableInfo tableInfo = session.getCatalog().getTable(database, table);
      Objects.nonNull(tableInfo);

      final TableSchema.Builder schemaBuilder = TableSchema.builder();
      tableInfo
          .getColumns()
          .forEach(
              col -> schemaBuilder.field(col.getName(), TypeUtils.getFlinkType(col.getType())));

      return SourceFunctionProvider.of(
          new FlinkTikvConsumer(
              conf,
              tableInfo,
              runtimeProviderContext.createTypeInformation(schemaBuilder.build().toRowDataType()),
              coordinator),
          false);
    } catch (final Throwable e) {
      throw new RuntimeException("Can't create consumer", e);
    }
  }
}
