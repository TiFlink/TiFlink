package org.tikv.cdc;

import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.TiRegion;
import org.tikv.kvproto.Cdcpb.ChangeDataEvent;
import org.tikv.kvproto.Cdcpb.ChangeDataRequest;
import org.tikv.kvproto.Cdcpb.Event;
import org.tikv.kvproto.ChangeDataGrpc;
import org.tikv.kvproto.ChangeDataGrpc.ChangeDataStub;
import org.tikv.kvproto.Metapb.Store;
import shade.io.grpc.ManagedChannel;
import shade.io.grpc.ManagedChannelBuilder;
import shade.io.grpc.stub.StreamObserver;

public class RegionCDCClient implements AutoCloseable, StreamObserver<ChangeDataEvent> {
    private final Logger logger = LoggerFactory.getLogger(RegionCDCClient.class);

    private final long startTs;
    private final TiRegion region;
    private final ManagedChannel channel;
    private final ChangeDataStub asyncStub;
    private final StreamObserver<Event> observer;

    private RegionCDCClient(
            final long startTs,
            final TiRegion region,
            final StreamObserver<Event> observer,
            final ManagedChannel channel
    ) {
        this.startTs = startTs;
        this.region = region;
        this.observer = observer;
        this.channel = channel;
        this.asyncStub = ChangeDataGrpc.newStub(channel);
    }

    public void start() {
        logger.info("start streaming (region: %d)", region.getId());
        final ChangeDataRequest request = ChangeDataRequest.newBuilder()
            .setRegionId(region.getId())
            .setCheckpointTs(startTs)
            .setStartKey(region.getStartKey())
            .setEndKey(region.getEndKey())
            .setRegionEpoch(region.getRegionEpoch())
            .build();
        asyncStub.eventFeed(request, this);
    }

    public TiRegion getRegion() {
        return region;
    }

    @Override
    public void onCompleted() {
        // should never be called
    }

    @Override
    public void onError(final Throwable e) {
        logger.error("error (region: %d)", region.getId());
        // TODO: pass error as an event?
        observer.onError(e);
    }

    @Override
    synchronized public void onNext(final ChangeDataEvent event) {
        for (final Event e: event.getEventsList()) {
            switch (e.getEventCase()) {
                case ADMIN:
                    logger.info("Admin event (region: %d)", region.getId());
                    break;
                case ERROR:
                    onError(new Throwable(e.getError().toString()));
                default:
                    observer.onNext(e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        channel.shutdown();
        try {
            logger.debug("awaitTermination (region: %d)", region.getId());
            channel.awaitTermination(60, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            channel.shutdownNow();
        }
        logger.info("terminated (region: %d)", region.getId());
    }

    static public class RegionCDCClientBuilder {
        final TiConfiguration conf;
        final RegionManager regionManager;

        public RegionCDCClientBuilder(final TiConfiguration conf, final RegionManager regionManager) {
            this.conf = conf;
            this.regionManager = regionManager;
        }

        public RegionCDCClient build(final long startTs, final TiRegion region, final StreamObserver<Event> observer) {
            final Store store = regionManager.getStoreById(region.getLeader().getStoreId());
            final URI uri = URI.create("http://" + store.getAddress());
            final ManagedChannel channel = ManagedChannelBuilder.forAddress(uri.getHost(), uri.getPort())
                .maxInboundMessageSize(conf.getMaxFrameSize())
                .build();
            return new RegionCDCClient(startTs, region, observer, channel);
        }
    }
}
