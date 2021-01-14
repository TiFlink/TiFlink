package org.tikv.flink.connectors;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;

public class TikvDynamicSink implements DynamicTableSink {

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DynamicTableSink copy() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String asSummaryString() {
    // TODO Auto-generated method stub
    return null;
  }
    
}
