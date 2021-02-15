package org.tikv.flink.connectors.coordinators;

import org.immutables.value.Value;

@Value.Immutable
public interface Transaction {
  public enum Status {
    NEW,
    PREWRITE,
    COMMITTED,
    ABORTED;
  };

  long getCheckpointId();

  long getStartTs();

  long getCommitTs();

  byte[] getPrimaryKey();

  Status getStatus();
}
