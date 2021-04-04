package org.tikv.flink.connectors.coordinator;

public interface CoordinatorProvider {
  Coordinator createCoordinator();
}
