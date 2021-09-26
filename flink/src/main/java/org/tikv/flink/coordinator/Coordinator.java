package org.tikv.flink.coordinator;

import java.io.Serializable;

public interface Coordinator extends AutoCloseable, Serializable {
  Transaction openTransaction(long checkpointId);

  Transaction prewriteTransaction(long checkpointId, long tableId);

  Transaction commitTransaction(long checkpointId);

  Transaction abortTransaction(long checkpointId);
}
