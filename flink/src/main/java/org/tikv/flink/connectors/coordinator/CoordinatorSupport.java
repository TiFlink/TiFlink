package org.tikv.flink.connectors.coordinator;

public interface CoordinatorSupport extends AutoCloseable {
  void start();
}
