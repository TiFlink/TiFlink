package org.tikv.flink.connectors.coordinators;

public interface SnapshotCoordinatorFactory {
  String factoryIdentifier();

  SnapshotCoordinator create(String taskId);
}
