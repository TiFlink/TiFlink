package org.tikv.flink.connectors.coordinator;

import java.util.Map;

public interface Provider extends AutoCloseable {
  void start();

  Map<String, String> getCoordinatorOptions();
}
