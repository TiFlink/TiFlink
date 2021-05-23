package org.tikv.flink.connectors.coordinator.grpc;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.util.Iterator;
import java.util.LinkedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.flink.connectors.coordinator.ImmutableTransaction;
import org.tikv.flink.connectors.coordinator.Transaction;
import org.tikv.flink.connectors.coordinator.grpc.Coordinator.TxnRequest;
import org.tikv.flink.connectors.coordinator.grpc.Coordinator.TxnResponse;
import org.tikv.txn.TwoPhaseCommitter;

class GrpcService extends CoordinatorServiceGrpc.CoordinatorServiceImplBase
    implements AutoCloseable {
  private static Logger logger = LoggerFactory.getLogger(GrpcService.class);

  private static byte[] EMPTY_VALUE = new byte[0];
  private static long TXN_TTL_MS = 20000;

  static final int RETAIN_TXNS = 100;

  private final LinkedHashMap<Long, TransactionHolder> transactions = new LinkedHashMap<>();
  private final TiSession tiSession;

  GrpcService(final TiSession session) {
    this.tiSession = session;
  }

  GrpcService(final TiConfiguration conf) {
    this(TiSession.create(conf));
  }

  protected Transaction openTransaction(final long checkpointId) {
    logger.info("open transaction: {}", checkpointId);
    synchronized (transactions) {
      final TransactionHolder holder =
          transactions.computeIfAbsent(checkpointId, (id) -> new TransactionHolder());
      trimTransactions();
      synchronized (holder) {
        if (holder.getTxn() == null) {
          holder.setTxn(
              ImmutableTransaction.builder()
                  .checkpointId(checkpointId)
                  .startTs(getTimestamp(checkpointId))
                  .build());
          return holder.getTxn();
        } else if (holder.getTxn().isNew()) {
          return holder.getTxn();
        } else {
          throw new IllegalStateException("Illegal transaction state");
        }
      }
    }
  }

  protected Transaction prewriteTransaction(final long checkpointId, final long tableId) {
    logger.info("prewrite transaction: {}, tableId: {}", checkpointId, tableId);
    final TransactionHolder holder = transactions.get(checkpointId);
    Preconditions.checkNotNull(holder, "Transaction not found");

    final Transaction txn = holder.getTxn();
    if (txn.isPrewriting()) {
      return txn;
    }

    synchronized (holder) {
      Preconditions.checkState(holder.getTxn().isNew(), "Transaction status should be NEW");
      final byte[] pk = GrpcCommitKey.encode(tableId, txn.getCheckpointId(), txn.getStartTs());
      prewritePrimaryKey(pk, holder.getCommitter());
      holder.setTxn(
          ImmutableTransaction.builder()
              .from(holder.getTxn())
              .primaryKey(pk)
              .status(Transaction.Status.PREWRITE)
              .build());
      return holder.getTxn();
    }
  }

  protected Transaction commitTransaction(final long checkpointId) {
    logger.info("commit transaction: {}", checkpointId);
    final TransactionHolder holder = transactions.get(checkpointId);
    Preconditions.checkNotNull(holder, "Transaction not found");

    synchronized (holder) {
      if (holder.getTxn().isCommitted()) {
        logger.info("transaction already committed: {}", checkpointId);
        return holder.getTxn();
      }

      if (holder.getTxn().isNew()) {
        // No prewrite has done. just return committed status
        holder.setTxn(
            ImmutableTransaction.builder()
                .from(holder.getTxn())
                .status(Transaction.Status.COMMITTED)
                .commitTs(getTimestamp(checkpointId))
                .build());
        logger.info("new transaction committed: {}", checkpointId);
        return holder.getTxn();
      }

      synchronized (transactions) {
        for (final TransactionHolder h : transactions.values()) {
          if (h != holder) {
            Preconditions.checkState(
                h.getTxn().isTerminated(),
                "Transaction started before this transaction must be terminated first");
          } else {
            break;
          }
        }
        trimTransactions();
      }
      final long commitTs = getTimestamp(checkpointId);
      commitPrimaryKey(holder.getTxn().getPrimaryKey(), commitTs, holder.getCommitter());

      holder.setTxn(
          ImmutableTransaction.builder()
              .from(holder.getTxn())
              .commitTs(commitTs)
              .status(Transaction.Status.COMMITTED)
              .build());

      logger.info("transaction committed: {}", holder.getTxn());
      return holder.getTxn();
    }
  }

  protected Transaction abortTransaction(final long checkpointId) {
    logger.info("abort transaction: {}", checkpointId);
    final TransactionHolder holder = transactions.get(checkpointId);
    if (holder == null) {
      // fail silently for unknown transactions
      return ImmutableTransaction.builder().checkpointId(checkpointId).build();
    }

    final Transaction txn = holder.getTxn();
    if (txn.isAborted()) {
      return txn;
    }

    synchronized (holder) {
      Preconditions.checkState(!holder.getTxn().isCommitted(), "Transaction is already committed");
      abortPrimaryKey(holder.getTxn().getPrimaryKey(), holder.getCommitter());
      holder.setTxn(
          ImmutableTransaction.builder()
              .from(holder.getTxn())
              .status(Transaction.Status.ABORTED)
              .build());
      return holder.getTxn();
    }
  }

  protected Transaction getTransaction(final long checkpointId) {
    final TransactionHolder holder = transactions.get(checkpointId);
    Preconditions.checkNotNull(holder, "Transaction not found");
    return holder.getTxn();
  }

  private long getTimestamp(final long checkpointId) {
    final TiTimestamp ts = tiSession.getTimestamp();
    return ts.getVersion();
  }

  private void prewritePrimaryKey(final byte[] primaryKey, final TwoPhaseCommitter committer) {
    committer.prewritePrimaryKey(ConcreteBackOffer.newRawKVBackOff(), primaryKey, EMPTY_VALUE);
  }

  private void commitPrimaryKey(
      final byte[] primaryKey, final long commitTs, final TwoPhaseCommitter committer) {
    committer.commitPrimaryKey(ConcreteBackOffer.newRawKVBackOff(), primaryKey, commitTs);
  }

  private void abortPrimaryKey(final byte[] primaryKey, final TwoPhaseCommitter committer) {
    // TODO: implement this
  }

  private void trimTransactions() {
    synchronized (transactions) {
      final Iterator<TransactionHolder> iter = transactions.values().iterator();
      while (transactions.size() > RETAIN_TXNS && iter.hasNext()) {
        final Transaction txn = iter.next().getTxn();
        if (txn.isTerminated()) {
          iter.remove();
        } else {
          return;
        }
      }
    }
  }

  public TxnResponse transactionToResponse(final Transaction txn) {
    final TxnResponse.Builder builder =
        TxnResponse.newBuilder()
            .setCheckpointId(txn.getCheckpointId())
            .setStartTs(txn.getStartTs());

    switch (txn.getStatus()) {
      case COMMITTED:
        builder.setCommitTs(txn.getCommitTs());
      case PREWRITE:
        builder.setPrimaryKey(ByteString.copyFrom(txn.getPrimaryKey()));
      default:
        builder.setStatus(convertStatus(txn.getStatus()));
    }

    return builder.build();
  }

  public TxnResponse.Status convertStatus(final Transaction.Status status) {
    switch (status) {
      case ABORTED:
        return TxnResponse.Status.ABORTED;
      case COMMITTED:
        return TxnResponse.Status.COMMITTED;
      case NEW:
        return TxnResponse.Status.NEW;
      case PREWRITE:
        return TxnResponse.Status.PREWRITE;
      default:
        throw new UnsupportedOperationException("Unknown status: " + status.toString());
    }
  }

  @Override
  public void transactions(
      final TxnRequest request, final StreamObserver<TxnResponse> responseObserver) {
    try {
      switch (request.getAction()) {
        case OPEN:
          responseObserver.onNext(
              transactionToResponse(openTransaction(request.getCheckpointId())));
          break;
        case PREWRITE:
          Preconditions.checkArgument(request.hasTableId(), "TableId can't be empty");
          responseObserver.onNext(
              transactionToResponse(
                  prewriteTransaction(request.getCheckpointId(), request.getTableId())));
          break;
        case COMMIT:
          responseObserver.onNext(
              transactionToResponse(commitTransaction(request.getCheckpointId())));
          break;
        case ABORT:
          responseObserver.onNext(
              transactionToResponse(abortTransaction(request.getCheckpointId())));
          break;
        case GET:
          responseObserver.onNext(transactionToResponse(getTransaction(request.getCheckpointId())));
          break;
        default:
          throw new UnsupportedOperationException();
      }
    } catch (final Throwable t) {
      logger.error("Server RPC error", t);
      responseObserver.onError(t);
    } finally {
      responseObserver.onCompleted();
    }
  }

  @Override
  public void close() throws Exception {
    tiSession.close();
  }

  class TransactionHolder implements AutoCloseable {
    private volatile Transaction txn = null;
    private TwoPhaseCommitter committer = null;

    public Transaction getTxn() {
      return txn;
    }

    public void setTxn(Transaction txn) {
      this.txn = txn;
    }

    public TwoPhaseCommitter getCommitter() {
      if (committer == null) {
        committer = new TwoPhaseCommitter(tiSession, txn.getStartTs(), TXN_TTL_MS);
      }
      return committer;
    }

    @Override
    public void close() throws Exception {
      if (committer != null) {
        committer.close();
      }
    }

    @Override
    protected void finalize() throws Throwable {
      close();
    }
  }
}
