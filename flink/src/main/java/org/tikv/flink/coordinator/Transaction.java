package org.tikv.flink.coordinator;

import org.immutables.value.Value;

@Value.Immutable
public interface Transaction {
  static final byte[] EMPTY_PK = new byte[0];

  public enum Status {
    NEW,
    PREWRITE,
    COMMITTED,
    ABORTED;
  };

  long getCheckpointId();

  long getStartTs();

  @Value.Default
  default long getCommitTs() {
    return 0;
  }

  @Value.Default
  default byte[] getPrimaryKey() {
    return EMPTY_PK;
  }

  @Value.Default
  default Status getStatus() {
    return Status.NEW;
  }

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
