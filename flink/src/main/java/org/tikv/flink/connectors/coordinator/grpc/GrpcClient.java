package org.tikv.flink.connectors.coordinator.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.tikv.flink.connectors.coordinator.Coordinator;
import org.tikv.flink.connectors.coordinator.ImmutableTransaction;
import org.tikv.flink.connectors.coordinator.Transaction;
import org.tikv.flink.connectors.coordinator.grpc.Coordinator.TxnRequest;
import org.tikv.flink.connectors.coordinator.grpc.Coordinator.TxnResponse;
import org.tikv.flink.connectors.coordinator.grpc.CoordinatorServiceGrpc.CoordinatorServiceBlockingStub;

class GrpcClient implements Coordinator {
  private static final long serialVersionUID = -6649512125783014469L;

  private final URI serverURI;

  private transient ManagedChannel channel;
  private transient CoordinatorServiceBlockingStub blockingStub;

  GrpcClient(final URI serverURI) {
    this.serverURI = serverURI;
  }

  @Override
  public void open() {
    channel = ManagedChannelBuilder.forAddress(serverURI.getHost(), serverURI.getPort()).build();
    blockingStub = CoordinatorServiceGrpc.newBlockingStub(channel);
  }

  @Override
  public void close() throws Exception {
    channel.awaitTermination(10, TimeUnit.SECONDS);
  }

  @Override
  public Transaction openTransaction(long checkpointId) {
    return call(
        TxnRequest.newBuilder()
            .setAction(TxnRequest.Action.OPEN)
            .setCheckpointId(checkpointId)
            .build());
  }

  @Override
  public Transaction prewriteTransaction(long checkpointId, long tableId) {
    return call(
        TxnRequest.newBuilder()
            .setAction(TxnRequest.Action.PRIWRITE)
            .setCheckpointId(checkpointId)
            .setTableId(tableId)
            .build());
  }

  @Override
  public Transaction commitTransaction(long checkpointId) {
    return call(
        TxnRequest.newBuilder()
            .setAction(TxnRequest.Action.COMMIT)
            .setCheckpointId(checkpointId)
            .build());
  }

  @Override
  public Transaction abortTransaction(long checkpointId) {
    return call(
        TxnRequest.newBuilder()
            .setAction(TxnRequest.Action.ABORT)
            .setCheckpointId(checkpointId)
            .build());
  }

  private Transaction call(final TxnRequest req) {
    final TxnResponse resp = blockingStub.transactions(req);
    final ImmutableTransaction.Builder txnBuilder = ImmutableTransaction.builder();

    txnBuilder.checkpointId(resp.getCheckpointId());
    txnBuilder.startTs(resp.getStartTs());

    if (resp.hasCommitTs()) {
      txnBuilder.commitTs(resp.getCommitTs());
    }

    if (resp.hasPrimaryKey()) {
      txnBuilder.primaryKey(resp.getPrimaryKey().toByteArray());
    }

    switch (resp.getStatus()) {
      case ABORTED:
        txnBuilder.status(Transaction.Status.ABORTED);
        break;
      case COMMITTED:
        txnBuilder.status(Transaction.Status.COMMITTED);
        break;
      case PRIWRITE:
        txnBuilder.status(Transaction.Status.PREWRITE);
        break;
      default:
        txnBuilder.status(Transaction.Status.NEW);
        break;
    }

    return txnBuilder.build();
  }
}
