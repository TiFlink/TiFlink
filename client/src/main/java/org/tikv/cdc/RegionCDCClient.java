package org.tikv.cdc;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.region.TiRegion;
import org.tikv.kvproto.Cdcpb.ChangeDataEvent;
import org.tikv.kvproto.Cdcpb.ChangeDataRequest;
import org.tikv.kvproto.Cdcpb.Header;
import org.tikv.kvproto.Cdcpb.ResolvedTs;
import org.tikv.kvproto.ChangeDataGrpc;
import org.tikv.kvproto.ChangeDataGrpc.ChangeDataStub;
import org.tikv.kvproto.Coprocessor.KeyRange;
import shade.io.grpc.ManagedChannel;
import shade.io.grpc.stub.StreamObserver;

public class RegionCDCClient implements AutoCloseable, StreamObserver<ChangeDataEvent> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RegionCDCClient.class);
  private static final AtomicLong reqIdCounter = new AtomicLong(0);

  private final long startTs;
  private final TiRegion region;
  private final KeyRange keyRange;
  private final KeyRange regionKeyRange;
  private final ManagedChannel channel;
  private final ChangeDataStub asyncStub;
  private final Consumer<CDCEvent> eventConsumer;
  private final AtomicBoolean running = new AtomicBoolean(false);

  public RegionCDCClient(
      final long startTs,
      final TiRegion region,
      final KeyRange keyRange,
      final ManagedChannel channel,
      final Consumer<CDCEvent> eventConsumer) {
    this.startTs = startTs;
    this.region = region;
    this.keyRange = keyRange;
    this.channel = channel;
    this.asyncStub = ChangeDataGrpc.newStub(channel);
    this.eventConsumer = eventConsumer;

    this.regionKeyRange =
        KeyRange.newBuilder().setStart(region.getStartKey()).setEnd(region.getEndKey()).build();
  }

  public void start() {
    LOGGER.info("start streaming (region: {})", region.getId());
    running.set(true);
    final ChangeDataRequest request =
        ChangeDataRequest.newBuilder()
            .setRequestId(reqIdCounter.incrementAndGet())
            .setHeader(Header.newBuilder().setTicdcVersion("5.0.0").build())
            .setRegionId(region.getId())
            .setCheckpointTs(startTs)
            .setStartKey(keyRange.getStart())
            .setEndKey(keyRange.getEnd())
            .setRegionEpoch(region.getRegionEpoch())
            .build();
    final StreamObserver<ChangeDataRequest> requestObserver = asyncStub.eventFeed(this);
    requestObserver.onNext(request);
  }

  public TiRegion getRegion() {
    return region;
  }

  public KeyRange getKeyRange() {
    return keyRange;
  }

  public KeyRange getRegionKeyRange() {
    return regionKeyRange;
  }

  public boolean isRunning() {
    return running.get();
  }

  @Override
  public void close() {
    running.set(false);
    channel.shutdown();
    try {
      LOGGER.debug("awaitTermination (region: {})", region.getId());
      channel.awaitTermination(60, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      channel.shutdownNow();
    }
    LOGGER.info("terminated (region: {})", region.getId());
  }

  @Override
  public void onCompleted() {
    // should never been called
    onError(new IllegalStateException("RegionCDCClient should never complete"));
  }

  @Override
  public void onError(final Throwable error) {
    LOGGER.error("region CDC error: region: {}, error: {}", region.getId(), error);
    eventConsumer.accept(CDCEvent.error(region.getId(), error));
    running.set(false);
  }

  @Override
  public void onNext(final ChangeDataEvent event) {
    if (running.get()) {
      event.getEventsList().stream()
          .flatMap(ev -> ev.getEntries().getEntriesList().stream())
          .map(row -> CDCEvent.rowEvent(region.getId(), row))
          .forEach(eventConsumer);

      if (event.hasResolvedTs()) {
        final ResolvedTs resolvedTs = event.getResolvedTs();
        if (resolvedTs.getRegionsList().indexOf(region.getId()) >= 0) {
          eventConsumer.accept(CDCEvent.resolvedTsEvent(region.getId(), resolvedTs.getTs()));
        }
      }
    }
  }
}
