package org.tikv.flink.connectors.coordinators;

public interface SnapshotCoordinator {
  long getInitTs();

  long getStartTs(long checkpointId);

  long getCommitTs(long checkpointId);

  byte[] getPrimaryKey(long checkpointId, byte[] proposedKey);

  boolean isCommitted(long checkpointId);
}
