package org.tikv.flink.connectors.coordinator.grpc;

import com.google.common.base.Preconditions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.flink.connectors.coordinator.Coordinator;
import org.tikv.flink.connectors.coordinator.ImmutableTransaction;
import org.tikv.flink.connectors.coordinator.Transaction;
import org.tikv.flink.connectors.coordinator.grpc.Coordinator.TxnRequest;
import org.tikv.flink.connectors.coordinator.grpc.Coordinator.TxnResponse;
import org.tikv.flink.connectors.coordinator.grpc.CoordinatorServiceGrpc.CoordinatorServiceBlockingStub;

class GrpcClient implements Coordinator {
  private static final long serialVersionUID = -6649512125783014469L;
  private static Logger logger = LoggerFactory.getLogger(GrpcClient.class);

  private final List<InetSocketAddress> serverAddresses;

  private transient ManagedChannel channel;
  private transient CoordinatorServiceBlockingStub blockingStub;

  GrpcClient(final List<InetSocketAddress> serverAddresses) {
    Preconditions.checkArgument(!serverAddresses.isEmpty(), "serverAddresses can't be empty");
    this.serverAddresses = serverAddresses;
  }

  public void open() {
    try {
      final InetSocketAddress address = getReachableServerAddress();
      logger.info("Client open connection to: {}", address);
      channel =
          ManagedChannelBuilder.forAddress(address.getHostName(), address.getPort())
              .usePlaintext()
              .build();
      blockingStub = CoordinatorServiceGrpc.newBlockingStub(channel);
    } catch (final IOException e) {
      throw new RuntimeException("Unable to open client", e);
    }
  }

  private InetSocketAddress getReachableServerAddress() throws IOException {
    for (final InetSocketAddress address : serverAddresses) {
      if (address.getAddress().isReachable(1000)) {
        return address;
      }
    }
    throw new IOException("Server is not reachable");
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
            .setAction(TxnRequest.Action.PREWRITE)
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
    if (blockingStub == null) {
      open();
    }
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
        Preconditions.checkState(resp.hasCommitTs());
        txnBuilder.status(Transaction.Status.COMMITTED);
        break;
      case PREWRITE:
        Preconditions.checkState(resp.hasPrimaryKey());
        txnBuilder.status(Transaction.Status.PREWRITE);
        break;
      default:
        txnBuilder.status(Transaction.Status.NEW);
        break;
    }

    return txnBuilder.build();
  }
}
