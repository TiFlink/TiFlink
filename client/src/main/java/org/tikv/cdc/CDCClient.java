package org.tikv.cdc;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Range;
import com.google.common.collect.TreeMultiset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.common.key.Key;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.RangeSplitter;
import org.tikv.common.util.RangeSplitter.RegionTask;
import org.tikv.kvproto.Cdcpb.Event.Row;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;
import shade.com.google.protobuf.ByteString;
import shade.io.grpc.ManagedChannel;

public class CDCClient implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(CDCClient.class);
  private static final int EVENT_BUFFER_SIZE = 10000;

  private final TiSession session;
  private final KeyRange keyRange;
  private final BlockingQueue<CDCEvent> eventsBuffer = new ArrayBlockingQueue<>(EVENT_BUFFER_SIZE);
  private final Map<Long, RegionCDCClient> regionClients = new HashMap<>();
  private final Map<Long, Long> regionToResolvedTs = new HashMap<>();
  private final TreeMultiset<Long> resolvedTsSet = TreeMultiset.create();

  private boolean started = false;

  public CDCClient(final TiSession session, final KeyRange keyRange) {
    Preconditions.checkState(
        session.getConf().getIsolationLevel().equals(IsolationLevel.SI),
        "Unsupported Isolation Level"); // only support SI for now
    this.session = session;
    this.keyRange = keyRange;
  }

  public synchronized void start(final long startTs) {
    Preconditions.checkState(!started, "Client is already started");
    try {
      applyKeyRange(keyRange, startTs);
    } catch (final Throwable e) {
      LOGGER.error("failed to start:", e);
    }
    started = true;
  }

  public synchronized Row get() throws InterruptedException {
    while (true) {
      final CDCEvent event = eventsBuffer.poll(100, TimeUnit.MILLISECONDS);
      if (event != null) {
        switch (event.eventType) {
          case ROW:
            return event.row;
          case RESOLVED_TS:
            handleResolvedTs(event.regionId, event.resolvedTs);
            break;
          case ERROR:
            handleErrorEvent(event.regionId, event.error);
            break;
        }
      }
    }
  }

  public synchronized long getMinResolvedTs() {
    return resolvedTsSet.firstEntry().getElement();
  }

  public synchronized long getMaxResolvedTs() {
    return resolvedTsSet.lastEntry().getElement();
  }

  public synchronized void close() {
    removeRegions(regionClients.keySet());
  }

  private synchronized void applyKeyRange(final KeyRange keyRange, final long timestamp) {
    final RangeSplitter splitter = RangeSplitter.newSplitter(session.getRegionManager());
    final List<TiRegion> regions =
        splitter.splitRangeByRegion(Arrays.asList(keyRange)).stream()
            .map(RegionTask::getRegion)
            .collect(Collectors.toList());

    final Set<Long> regionsToKeep =
        regions.stream()
            .filter(this::shouldKeepRegion)
            .map(TiRegion::getId)
            .collect(Collectors.toSet());

    final Set<Long> regionsToRemove =
        regionClients.keySet().stream()
            .filter(Predicates.not(regionsToKeep::contains))
            .collect(Collectors.toSet());

    final List<TiRegion> regionsToAdd =
        regions.stream()
            .filter(region -> !regionsToKeep.contains(region.getId()))
            .collect(Collectors.toList());

    removeRegions(regionsToRemove);
    addRegions(regionsToAdd, timestamp);
  }

  private synchronized void addRegions(final Iterable<TiRegion> regions, final long timestamp) {
    for (final TiRegion region : regions) {
      final Optional<KeyRange> rangeOpt = regionRangeIntersection(region);
      if (rangeOpt.isPresent()) {
        final String address = session.getRegionManager().getStoreById(region.getId()).getAddress();
        final ManagedChannel channel = session.getChannelFactory().getChannel(address);
        try (final RegionCDCClient client =
            new RegionCDCClient(timestamp, region, rangeOpt.get(), channel, eventsBuffer::offer)) {

          regionClients.put(region.getId(), client);
          regionToResolvedTs.put(region.getId(), timestamp);
          resolvedTsSet.add(timestamp);

          client.start();
        }
      }
    }
  }

  private synchronized void removeRegions(final Iterable<Long> regionIds) {
    for (final long regionId : regionIds) {
      if (regionClients.containsKey(regionId)) {
        final RegionCDCClient regionClient = regionClients.remove(regionId);
        resolvedTsSet.remove(regionToResolvedTs.remove(regionId));
        try {
          regionClient.close();
        } catch (final Exception e) {
          LOGGER.error("failed to close region client, region id: {}, error: {}", regionId, e);
        }
      }
    }
  }

  private Optional<KeyRange> regionRangeIntersection(final TiRegion region) {
    final Range<Key> regionRange =
        Range.closedOpen(Key.toRawKey(region.getStartKey()), Key.toRawKey(region.getEndKey()));
    final Range<Key> clientRange =
        Range.closedOpen(Key.toRawKey(keyRange.getStart()), Key.toRawKey(keyRange.getEnd()));
    final Range<Key> intersection = regionRange.intersection(clientRange);
    if (intersection.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(
          KeyRange.newBuilder()
              .setStart(ByteString.copyFrom(intersection.lowerEndpoint().getBytes()))
              .setEnd(ByteString.copyFrom(intersection.upperEndpoint().getBytes()))
              .build());
    }
  }

  private void handleResolvedTs(final long regionId, final long resolvedTs) {
    resolvedTsSet.remove(regionToResolvedTs.get(regionId));
    resolvedTsSet.add(resolvedTs);
  }

  private void handleErrorEvent(final long regionId, final Throwable error) {
    final long resolvedTs = regionToResolvedTs.get(regionId);
    final TiRegion region = regionClients.get(regionId).getRegion();

    session.getRegionManager().onRequestFail(region); // invalidate cache for corresponding region

    removeRegions(Arrays.asList(regionId));
    applyKeyRange(keyRange, resolvedTs); // reapply the whole keyRange
  }

  private boolean shouldKeepRegion(final TiRegion region) {
    final RegionCDCClient oldClient = regionClients.get(region.getId());
    if (oldClient != null && oldClient.isRunning()) {
      final KeyRange newKeyRange =
          KeyRange.newBuilder().setStart(region.getStartKey()).setEnd(region.getEndKey()).build();
      return oldClient.getRegionKeyRange().equals(newKeyRange);
    }
    return false;
  }
}
