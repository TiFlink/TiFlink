package org.tikv.flink.connectors;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.FlinkException;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ExternallyInducedSource.CheckpointTrigger;

public class FlinkTikvConsumer<T> extends RichParallelSourceFunction<T>
    implements CheckpointListener, CheckpointedFunction, CheckpointTrigger {

  private static final long serialVersionUID = 8647392870748599256L;

  @Override
  public void run(final SourceContext<T> ctx) throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void cancel() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void triggerCheckpoint(long checkpointId) throws FlinkException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    // TODO Auto-generated method stub
    
  }
    
}
