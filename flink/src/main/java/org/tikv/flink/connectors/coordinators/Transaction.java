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

  default boolean isTerminated() {
    switch (getStatus()) {
      case ABORTED:
      case COMMITTED:
        return true;
      default:
        return false;
    }
  }

  default boolean isNew() {
    return getStatus() == Status.NEW;
  }

  default boolean isPrewriting() {
    return getStatus() == Status.PREWRITE;
  }

  default boolean isCommitted() {
    return getStatus() == Status.COMMITTED;
  }

  default boolean isAborted() {
    return getStatus() == Status.ABORTED;
  }
}
