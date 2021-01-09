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

public class RegionCDCClient extends AbstractGRPCClient<ChangeDataBlockingStub, ChangeDataStub> {
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

        public RegionCDCClient build(final Store store, final long regionId) {
            final ManagedChannel channel = channelFactory.getChannel(store.getAddress());
            final ChangeDataBlockingStub blockingStub = ChangeDataGrpc.newBlockingStub(channel);
            final ChangeDataStub asyncStub = ChangeDataGrpc.newStub(channel);
            return new RegionCDCClient(conf, channelFactory, blockingStub, asyncStub, regionId);
        }

        public RegionCDCClient build(final long storeId, final long regionId) {
            return build(regionManager.getStoreById(storeId), regionId);
        }

        public RegionCDCClient build(final TiRegion region) {
            return build(region.getLeader().getStoreId(), region.getId());
        }
    }

    private final long regionId;
    private final ChangeDataBlockingStub blockingStub;
    private final ChangeDataStub asyncStub;

    private RegionCDCClient(
            final TiConfiguration conf,
            final ChannelFactory channelFactory,
            final ChangeDataBlockingStub blockingStub,
            final ChangeDataStub asyncStub,
            final long regionId
    ) {
        super(conf, channelFactory);
        this.blockingStub = blockingStub;
        this.asyncStub = asyncStub;
        this.regionId = regionId;
    }

    public ChangeDataEvent getChangeDataEvent(final BackOffer backOffer,final long startTs) {
        final ChangeDataRequest request = ChangeDataRequest.newBuilder()
            .setRegionId(regionId)
            .setCheckpointTs(startTs)
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
}
