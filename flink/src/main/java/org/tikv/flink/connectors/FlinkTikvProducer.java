package org.tikv.flink.connectors;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.table.data.RowData;

public class FlinkTikvProducer
    extends TwoPhaseCommitSinkFunction<RowData,
            FlinkTikvProducer.TikvTransactionState,
            FlinkTikvProducer.TikvTransactionContext> {

  private static final long serialVersionUID = -3795270131952943711L;
    
    public FlinkTikvProducer(TypeSerializer<TikvTransactionState> transactionSerializer,
      TypeSerializer<TikvTransactionContext> contextSerializer) {
    super(transactionSerializer, contextSerializer);
    //TODO Auto-generated constructor stub
  }


    public static class TikvTransactionState {
    }

    public static class TikvTransactionContext {
    }

    @Override
    protected void invoke(TikvTransactionState transaction, RowData value, Context context)
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
