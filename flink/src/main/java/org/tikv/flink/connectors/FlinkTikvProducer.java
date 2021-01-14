package org.tikv.flink.connectors;

import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

public class FlinkTikvProducer<T>
    extends TwoPhaseCommitSinkFunction<T,
            FlinkTikvProducer.TikvTransactionState,
            FlinkTikvProducer.TikvTransactionContext> {
    
    public static class TikvTransactionState {
    }

    public static class TikvTransactionContext {
    }

    @Override
    protected void invoke(TikvTransactionState transaction, T value, Context context)
        throws Exception {
      // TODO Auto-generated method stub
      
    }

    @Override
    protected TikvTransactionState beginTransaction() throws Exception {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected void preCommit(TikvTransactionState transaction) throws Exception {
      // TODO Auto-generated method stub
      
    }

    @Override
    protected void commit(TikvTransactionState transaction) {
      // TODO Auto-generated method stub
      
    }

    @Override
    protected void abort(TikvTransactionState transaction) {
      // TODO Auto-generated method stub
      
    }
}
