package org.tikv.cdc;

import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.RegionCDCClient.RegionCDCClientBuilder;
import org.tikv.common.TiConfiguration;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Cdcpb.ChangeDataEvent;
import org.tikv.kvproto.Cdcpb.Event;
import org.tikv.kvproto.Cdcpb.Event.Row;

public class CDCClient implements AutoCloseable {
    private final TiConfiguration conf;
    private final RegionCDCClientBuilder clientBuilder;
    private final Logger logger = LoggerFactory.getLogger(CDCClient.class);
    private final TreeMap<TiRegion, RegionCDCClient> regionClients;
    private final PriorityQueue<RegionState> regionStates;

    public CDCClient(final TiConfiguration conf, final RegionCDCClientBuilder clientBuilder,
            final List<TiRegion> regions, final long startTs) {
        this.conf = conf;
        this.clientBuilder = clientBuilder; 
        this.regionClients = new TreeMap<>((a, b) -> a.getRowEndKey().compareTo(b.getRowEndKey()));
        this.regionStates = new PriorityQueue<>(
            regions.stream()
                .map((r) -> RegionState.create(r, startTs))
                .collect(Collectors.toList())
        );
    }

    synchronized public void close() {
        for (RegionCDCClient client : regionClients.values()) {
            try {
              client.close();
            } catch (final Exception e) {
                logger.error("failed to close RegionCDCClient", e);
            }
        }
    }

    synchronized public List<Row> pullRows() {
        if (regionStates.isEmpty()) {
            return Collections.emptyList();
        }

        final RegionState next = regionStates.poll();
        final RegionCDCClient client = regionClients.computeIfAbsent(next.region, clientBuilder::build);
        final ChangeDataEvent cdcEvent = client.getChangeDataEvent(ConcreteBackOffer.newGetBackOff(), next.timestamp);

        final List<Event> result = cdcEvent.getEventsList();
        // TODO: implement this method
        return Collections.emptyList();
    }

    synchronized public long getTimestamp() {
        return regionStates.stream().mapToLong(r -> r.timestamp).min().orElse(0);
    }

    synchronized public long getResolvedTs() {
        return regionStates.stream().mapToLong(r -> r.timestamp).min().orElse(0);
    }

    static private class RegionState implements Comparable<RegionState> {
        final TiRegion region;
        final long timestamp;
        final long resolvedTs;

        private RegionState(final TiRegion region, final long timestamp) {
            this(region, timestamp, timestamp);
        }

        private RegionState(final TiRegion region, final long timestamp, final long resolvedTs) {
            this.region = region;
            this.timestamp = Math.max(timestamp, resolvedTs);
            this.resolvedTs = resolvedTs;
        }

        public RegionState with(final long timestamp, final long resolvedTs) {
            return new RegionState(region, timestamp, resolvedTs);
        }

        public RegionState withTimestamp(final long timestamp) {
            return new RegionState(region, timestamp, resolvedTs);
        }

        public RegionState withResolvedTs(final long resolvedTs) {
            return new RegionState(region, timestamp, resolvedTs);
        }

        static public RegionState create(final TiRegion region, final long timestamp) {
            return new RegionState(region, timestamp);
        }

        @Override
        public int compareTo(final RegionState that) {
            return Long.compare(this.timestamp, that.timestamp);
        }
    }
}
