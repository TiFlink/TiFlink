package org.tikv.flink.connectors;

import java.util.Map;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.flink.connectors.coordinator.Coordinator;

public class TikvDynamicSink implements DynamicTableSink {

  private final String database;
  private final String table;
  private final ResolvedSchema schema;
  private final Map<String, String> options;

  public TikvDynamicSink(
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
  public ChangelogMode getChangelogMode(final ChangelogMode requestedMode) {
    return ChangelogMode.newBuilder()
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.DELETE)
        .addContainedKind(RowKind.UPDATE_AFTER)
        .build();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(final Context context) {
    final TiConfiguration tiConf = TiFlinkOptions.getTiConfiguration(options);
    try (final TiSession session = TiSession.create(tiConf)) {
      final TiTableInfo tableInfo = session.getCatalog().getTable(database, table);
      final Coordinator coordinator = TiFlinkOptions.getCoordinator(options);
      return SinkFunctionProvider.of(
          new FlinkTikvProducer(tiConf, tableInfo, schema.toPhysicalRowDataType(), coordinator));
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DynamicTableSink copy() {
    return new TikvDynamicSink(database, table, schema, options);
  }

  @Override
  public String asSummaryString() {
    return String.format("TiKV Table[`%s`.`%s`]", database, table);
  }
}
