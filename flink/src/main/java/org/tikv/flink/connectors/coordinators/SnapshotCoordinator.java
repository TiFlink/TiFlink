package org.tikv.flink.connectors.coordinators;

public interface SnapshotCoordinator extends AutoCloseable {
  Transaction openTransaction(long checkpointId);

  Transaction prewriteTransaction(long checkpointId, long tableId);

  Transaction commitTransaction(long checkpointId);

  Transaction abortTransaction(long checkpointId);
}
