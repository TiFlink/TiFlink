package org.tikv.cdc;

import org.tikv.kvproto.ChangeDataGrpc.ChangeDataStub;
import org.tikv.kvproto.Metapb.Store;
import shade.io.grpc.ManagedChannel;
import org.tikv.kvproto.ChangeDataGrpc;
import org.tikv.kvproto.Cdcpb.ChangeDataEvent;
import org.tikv.kvproto.Cdcpb.ChangeDataRequest;
import org.tikv.kvproto.ChangeDataGrpc.ChangeDataBlockingStub;
import org.tikv.common.AbstractGRPCClient;
import org.tikv.common.PDClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.ConcreteBackOffer;

public class RegionCDCClient extends AbstractGRPCClient<ChangeDataBlockingStub, ChangeDataStub> {
    private final TiRegion region;
    private final ChangeDataBlockingStub blockingStub;
    private final ChangeDataStub asyncStub;

    private RegionCDCClient(
            final TiConfiguration conf,
            final ChannelFactory channelFactory,
            final ChangeDataBlockingStub blockingStub,
            final ChangeDataStub asyncStub,
            final TiRegion region
    ) {
        super(conf, channelFactory);
        this.blockingStub = blockingStub;
        this.asyncStub = asyncStub;
        this.region = region;
    }

    public ChangeDataEvent getChangeDataEvent(final BackOffer backOffer,final long startTs) {
        final ChangeDataRequest request = ChangeDataRequest.newBuilder()
            .setRegionId(region.getId())
            .setCheckpointTs(startTs)
            .setStartKey(region.getStartKey())
            .setEndKey(region.getEndKey())
            .setRegionEpoch(region.getRegionEpoch())
            .build();
        return callWithRetry(backOffer, ChangeDataGrpc.getEventFeedMethod(), () -> request, null);
    }

    @Override
    protected ChangeDataBlockingStub getBlockingStub() {
        return blockingStub;
    }

    @Override
    protected ChangeDataStub getAsyncStub() {
        return asyncStub;
    }

    @Override
    public void close() throws Exception {
    }

    static public class RegionCDCClientBuilder {
        final TiConfiguration conf;
        final ChannelFactory channelFactory;
        final RegionManager regionManager;
        final PDClient pdClient;

        public RegionCDCClientBuilder(
                final TiConfiguration conf, 
                final ChannelFactory channelFactory,
                final RegionManager regionManager,
                final PDClient pdClient
        ) {
            this.conf = conf;
            this.channelFactory = channelFactory;
            this.regionManager = regionManager;
            this.pdClient = pdClient;
        }

        public RegionCDCClient build(final TiRegion region) {
            final Store store = regionManager.getStoreById(region.getLeader().getStoreId());
            final ManagedChannel channel = channelFactory.getChannel(store.getAddress());
            final ChangeDataBlockingStub blockingStub = ChangeDataGrpc.newBlockingStub(channel);
            final ChangeDataStub asyncStub = ChangeDataGrpc.newStub(channel);
            return new RegionCDCClient(conf, channelFactory, blockingStub, asyncStub, region);
        }

        public RegionCDCClient build(final long regionId) {
            return build(pdClient.getRegionByID(ConcreteBackOffer.newGetBackOff(), regionId));
        }
    }
}
