package org.tikv.flink.coordinator;

import java.util.Map;

public interface Provider extends AutoCloseable {
  void start();

  Map<String, String> getCoordinatorOptions();
}
