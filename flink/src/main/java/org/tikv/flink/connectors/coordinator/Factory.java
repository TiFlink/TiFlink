package org.tikv.flink.connectors.coordinator;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.ServiceLoader;

public interface Factory {
  String identifier();

  Provider createProvider(Map<String, String> options);

  Coordinator createCoordinator(Map<String, String> options);

  public static Factory getFactory(final String identifier) {
    final ServiceLoader<Factory> loader = ServiceLoader.load(Factory.class);
    final Iterator<Factory> iter = loader.iterator();
    while (iter.hasNext()) {
      final Factory factory = iter.next();
      if (Objects.equals(identifier, factory.identifier())) {
        return factory;
      }
    }
    throw new NoSuchElementException("Factory not found: " + identifier);
  }
}
