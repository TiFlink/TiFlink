package org.tikv.cdc;

import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.ChannelFactory;
import org.tikv.kvproto.Cdcpb.ChangeDataEvent;
import org.tikv.kvproto.Cdcpb.ChangeDataRequest;
import org.tikv.kvproto.Cdcpb.Header;
import org.tikv.kvproto.ChangeDataGrpc;
import org.tikv.kvproto.ChangeDataGrpc.ChangeDataStub;
import org.tikv.kvproto.Metapb.Store;
import shade.io.grpc.ManagedChannel;
import shade.io.grpc.ManagedChannelBuilder;
import shade.io.grpc.stub.StreamObserver;

public class RegionCDCClient implements AutoCloseable {
    private static final AtomicLong reqIdCounter = new AtomicLong(0);
    private final Logger logger = LoggerFactory.getLogger(RegionCDCClient.class);

    private final long startTs;
    private final TiRegion region;
    private final ManagedChannel channel;
    private final ChangeDataStub asyncStub;
    private final StreamObserver<ChangeDataEvent> observer;

    private RegionCDCClient(
            final long startTs,
            final TiRegion region,
            final StreamObserver<ChangeDataEvent> observer,
            final ManagedChannel channel
    ) {
        this.startTs = startTs;
        this.region = region;
        this.observer = observer;
        this.channel = channel;
        this.asyncStub = ChangeDataGrpc.newStub(channel);
    }

    public void start() {
        logger.info("start streaming (region: {})", region.getId());
        final ChangeDataRequest request = ChangeDataRequest.newBuilder()
            .setRequestId(reqIdCounter.incrementAndGet())
            .setHeader(Header.newBuilder().setTicdcVersion("4.0.0").build())
            .setRegionId(region.getId())
            .setCheckpointTs(startTs)
            .setStartKey(region.getStartKey())
            .setEndKey(region.getEndKey())
            .setRegionEpoch(region.getRegionEpoch())
            .build();
        final StreamObserver<ChangeDataRequest> requestObserver = asyncStub.eventFeed(observer);
        requestObserver.onNext(request);
    }

    public TiRegion getRegion() {
        return region;
    }

    @Override
    public void close() throws Exception {
        channel.shutdown();
        try {
            logger.debug("awaitTermination (region: {})", region.getId());
            channel.awaitTermination(60, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            channel.shutdownNow();
        }
        logger.info("terminated (region: {})", region.getId());
    }

    static public class RegionCDCClientBuilder {
        final TiConfiguration conf;
        final RegionManager regionManager;
        final ChannelFactory channelFactory;

        public RegionCDCClientBuilder(
                final TiConfiguration conf, final RegionManager regionManager, final ChannelFactory channelFactory) {
            this.conf = conf;
            this.regionManager = regionManager;
            this.channelFactory = channelFactory;
        }

        public RegionCDCClient build(final long startTs, final TiRegion region, final StreamObserver<ChangeDataEvent> observer) {
            final Store store = regionManager.getStoreById(region.getLeader().getStoreId());
            return new RegionCDCClient(startTs, region, observer, channelFactory.getChannel(store.getAddress()));
        }
    }
}
