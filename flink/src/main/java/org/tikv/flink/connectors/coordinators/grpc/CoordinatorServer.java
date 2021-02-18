package org.tikv.flink.connectors.coordinators.grpc;

import io.grpc.stub.StreamObserver;
import org.tikv.flink.connectors.coordinators.grpc.CoordinatorOuterClass.TransactionRequest;
import org.tikv.flink.connectors.coordinators.grpc.CoordinatorOuterClass.TransactionResponse;

public class CoordinatorServer extends CoordinatorGrpc.CoordinatorImplBase {

  @Override
  public void transaction(
      TransactionRequest request, StreamObserver<TransactionResponse> responseObserver) {
    try {
      switch (request.getAction()) {
        case OPEN:
          break;
        case ABORT:
          break;
        case COMMIT:
          break;
        case GET:
          break;
        case PRIWRITE:
          break;
        default:
          throw new UnsupportedOperationException();
      }
    } catch (final Throwable t) {
      responseObserver.onError(t);
    } finally {
      responseObserver.onCompleted();
    }
  }

  protected void openTransaction(final long checkpointId) {}

  protected void prewriteTransaction(final long checkpointId) {}

  protected void commitTransaction(final long checkpointId) {}

  protected void abortTransaction(final long checkpointId) {}

  protected void getTransaction(final long checkpointId) {}
}
