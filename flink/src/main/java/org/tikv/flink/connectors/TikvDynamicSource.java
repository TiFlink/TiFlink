package org.tikv.flink.connectors;

import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.flink.connectors.coordinator.Coordinator;

public class TikvDynamicSource implements ScanTableSource {

  private final String database;
  private final String table;
  private final ResolvedSchema schema;
  private final Map<String, String> options;

  public TikvDynamicSource(
      final String database,
      final String table,
      final ResolvedSchema schema,
      final Map<String, String> options) {
    this.database = database;
    this.table = table;
    this.schema = schema;
    this.options = options;
  }

  @Override
  public DynamicTableSource copy() {
    return new TikvDynamicSource(database, table, schema, options);
  }

  @Override
  public String asSummaryString() {
    return String.format("TiKV Table[`%s`.`%s`]{%s}", database, table);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.newBuilder()
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.DELETE)
        .addContainedKind(RowKind.UPDATE_AFTER)
        .build();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(final ScanContext context) {
    final TiConfiguration tiConf = TiFlinkOptions.getTiConfiguration(options);
    try (final TiSession session = TiSession.create(tiConf)) {
      final TiTableInfo tableInfo = session.getCatalog().getTable(database, table);
      final TypeInformation<RowData> typeInfo =
          context.createTypeInformation(schema.toSourceRowDataType());
      final Coordinator coordinator = TiFlinkOptions.getCoordinator(options);
      return SourceFunctionProvider.of(
          new FlinkTikvConsumer(tiConf, tableInfo, typeInfo, coordinator), false);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}
