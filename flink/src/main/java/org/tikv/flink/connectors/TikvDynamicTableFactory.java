package org.tikv.flink.connectors;

import java.util.Collections;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

public class TikvDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
  public static final String IDENTIFIER = "tiflink";

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.emptySet();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Set.of(
        TiFlinkOptions.TIKV_PD_ADDRESSES,
        TiFlinkOptions.TIKV_GRPC_TIMEOUT,
        TiFlinkOptions.TIKV_GRPC_SCAN_TIMEOUT,
        TiFlinkOptions.TIKV_BATCH_GET_CONCURRENCY,
        TiFlinkOptions.TIKV_BATCH_PUT_CONCURRENCY,
        TiFlinkOptions.TIKV_BATCH_SCAN_CONCURRENCY,
        TiFlinkOptions.TIKV_BATCH_DELETE_CONCURRENCY,
        TiFlinkOptions.COORDINATOR_PROVIDER
    );
  }

  @Override
  public DynamicTableSink createDynamicTableSink(final Context context) {
    final ObjectIdentifier tableIdent = context.getObjectIdentifier();
    return new TikvDynamicSink(
        tableIdent.getDatabaseName(),
        tableIdent.getObjectName(),
        context.getCatalogTable().getResolvedSchema(),
        context.getCatalogTable().getOptions());
  }

  @Override
  public DynamicTableSource createDynamicTableSource(final Context context) {
    final ObjectIdentifier tableIdent = context.getObjectIdentifier();
    return new TikvDynamicSource(
        tableIdent.getDatabaseName(),
        tableIdent.getObjectName(),
        context.getCatalogTable().getResolvedSchema(),
        context.getCatalogTable().getOptions());
  }
}
