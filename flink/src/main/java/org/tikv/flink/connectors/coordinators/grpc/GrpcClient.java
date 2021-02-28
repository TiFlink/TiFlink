package org.tikv.flink.connectors.coordinators.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.tikv.flink.connectors.coordinators.ImmutableTransaction;
import org.tikv.flink.connectors.coordinators.SnapshotCoordinator;
import org.tikv.flink.connectors.coordinators.Transaction;
import org.tikv.flink.connectors.coordinators.grpc.Coordinator.TxnRequest;
import org.tikv.flink.connectors.coordinators.grpc.Coordinator.TxnResponse;
import org.tikv.flink.connectors.coordinators.grpc.CoordinatorServiceGrpc.CoordinatorServiceBlockingStub;

class GrpcClient implements SnapshotCoordinator {
  private final ManagedChannel channel;
  private final CoordinatorServiceBlockingStub blockingStub;

  GrpcClient(final ManagedChannel channel) {
    this.channel = channel;
    blockingStub = CoordinatorServiceGrpc.newBlockingStub(channel);
  }

  GrpcClient(final String host, final int port) {
    this(ManagedChannelBuilder.forAddress(host, port).build());
  }

  GrpcClient(final URI serverURI) {
    this(serverURI.getHost(), serverURI.getPort());
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
