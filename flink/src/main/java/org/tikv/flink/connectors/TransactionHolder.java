package org.tikv.flink.connectors;

import java.io.Serializable;
import org.tikv.flink.connectors.coordinator.Transaction;

class TransactionHolder implements Transaction, Serializable {
  private static final long serialVersionUID = 1L;

  private Transaction transaction = null;

  synchronized void set(final Transaction txn) {
    transaction = txn;
  }

  synchronized Transaction get() {
    return transaction;
  }

  @Override
  public synchronized long getCheckpointId() {
    return transaction.getCheckpointId();
  }

  @Override
  public synchronized long getStartTs() {
    return transaction.getStartTs();
  }

  @Override
  public synchronized long getCommitTs() {
    return transaction.getCommitTs();
  }

  @Override
  public synchronized byte[] getPrimaryKey() {
    return transaction.getPrimaryKey();
  }

  @Override
  public synchronized Status getStatus() {
    return transaction.getStatus();
  }
}
