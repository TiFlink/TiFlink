package org.tikv.flink.connectors;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.types.RowKind;

public class TikvDynamicSource implements ScanTableSource {

  @Override
  public DynamicTableSource copy() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String asSummaryString() {
    return "TiKV dynamic table source";
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.newBuilder()
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.DELETE)
        .build();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
    return null;
  }
    
}
