package org.tikv.cdc;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.RegionCDCClient.RegionCDCClientBuilder;
import org.tikv.common.TiConfiguration;
import org.tikv.common.region.TiRegion;
import org.tikv.kvproto.Cdcpb.ChangeDataEvent;
import org.tikv.kvproto.Cdcpb.Event;
import org.tikv.kvproto.Cdcpb.ResolvedTs;
import org.tikv.kvproto.Cdcpb.Event.LogType;
import org.tikv.kvproto.Cdcpb.Event.Row;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;
import shade.io.grpc.stub.StreamObserver;

public class CDCClient implements AutoCloseable, StreamObserver<ChangeDataEvent> {
    private final Logger logger = LoggerFactory.getLogger(CDCClient.class);

    private final boolean started = false;
    private final RegionStateManager rsManager;
    private final List<RegionCDCClient> regionClients;
    private final BlockingQueue<ChangeDataEvent> eventsBuffer;
    private ChangeDataEvent currentCDCEvent = null;
    private Event currentEvent = null;
    private Iterator<Event> eventIter = null;
    private Iterator<Row> rowIter = null;

    public CDCClient(final TiConfiguration conf, final RegionCDCClientBuilder clientBuilder,
            final List<TiRegion> regions, final long startTs) {
        assert(conf.getIsolationLevel().equals(IsolationLevel.SI)); // only support SI for now
        this.rsManager = new RegionStateManager(regions);
        this.regionClients = regions.stream()
            .map(region -> clientBuilder.build(startTs, region, this))
            .collect(Collectors.toList());

        // TODO: Add a dedicated config to TiConfiguration for queue size.
        this.eventsBuffer = new ArrayBlockingQueue<>(conf.getTableScanConcurrency() * conf.getScanBatchSize());
    }

    synchronized public void start() {
        if (!started) {
            try {
                for (final RegionCDCClient client: regionClients) {
                    client.start();
                }
            } catch (final Throwable e) {
                logger.error("failed to start:", e);
            }
        }
    }

    synchronized public Row get() throws InterruptedException {
        while (true) {
            if (rowIter != null && rowIter.hasNext()) {
                final Row row = rowIter.next();
                if (row.getType() == LogType.INITIALIZED) {
                    rsManager.markInitialized(currentEvent.getRegionId());
                    continue;
                }

                return row;
            }

            if (eventIter != null && eventIter.hasNext()) {
                currentEvent = eventIter.next();
                rowIter = currentEvent.getEntries().getEntriesList().iterator();
                continue;
            }


            if (currentCDCEvent != null) {
                final ResolvedTs resolvedTs = currentCDCEvent.getResolvedTs();
                final long ts = resolvedTs.getTs();
                resolvedTs.getRegionsList().forEach(rid -> rsManager.updateTs(rid, ts));
            }


            currentCDCEvent = null;
            currentEvent = null;
            eventIter = null;
            rowIter = null;

            for (int i = 0; i < 10 && currentCDCEvent == null; i++) {
                currentCDCEvent = eventsBuffer.poll(100, TimeUnit.MILLISECONDS);
            }

            if (currentCDCEvent != null) {
                eventIter = currentCDCEvent.getEventsList().iterator();
            } else {
                // don't block worker forever
                return null;
            }
        }
    }

    synchronized public void close() {
        for (final RegionCDCClient client : regionClients) {
            try {
                client.close();
            } catch (final Throwable e) {
                logger.error(String.format("failed to close region client {}", client.getRegion().getId()), e);
            }
        }
    }

    synchronized public long getMinResolvedTs() {
        return rsManager.getMinTs();
    }

    synchronized public long getMaxResolvedTs() {
        return rsManager.getMaxTs();
    }

    synchronized public boolean isStarted() {
        return started;
    }

    synchronized public boolean isInitialized() {
        return rsManager.isInitialized();
    }

    @Override
    public void onCompleted() {
        // should never trigger for CDC request
        logger.error("should not completed!");
    }

    @Override
    public void onError(final Throwable err) {
        logger.error("error:", err);
    }

    @Override
    public void onNext(final ChangeDataEvent event) {
        logger.debug("buffer event: {}", event);
        eventsBuffer.offer(event);
    }
}
