package org.tikv.flink.connectors.coordinator;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.ServiceLoader;

public interface CoordinatorProvider {
  String identifier();

  Coordinator createCoordinator(Map<String, String> options);

  CoordinatorSupport createSupport(Map<String, String> options);

  public static CoordinatorProvider get(final String name) {
    final ServiceLoader<CoordinatorProvider> serviceLoader =
        ServiceLoader.load(CoordinatorProvider.class);
    
    final Iterator<CoordinatorProvider> providers = serviceLoader.iterator();
    while (providers.hasNext()) {
      final CoordinatorProvider provider = providers.next();
      if (Objects.equals(provider.identifier(), name)) {
        return provider;
      }
    }

    throw new NoSuchElementException();
  }
}
