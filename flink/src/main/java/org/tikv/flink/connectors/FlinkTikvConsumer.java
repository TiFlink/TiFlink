package org.tikv.flink.connectors;

import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ExternallyInducedSource.CheckpointTrigger;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.CDCClient;
import org.tikv.cdc.RegionCDCClient.RegionCDCClientBuilder;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.codec.TiTableCodec;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.region.TiRegion;
import org.tikv.kvproto.Cdcpb.Event.Row;

public class FlinkTikvConsumer extends RichParallelSourceFunction<RowData>
    implements CheckpointListener, CheckpointedFunction, CheckpointTrigger {
    private static final long serialVersionUID = 8647392870748599256L;

    private Logger logger = LoggerFactory.getLogger(FlinkTikvConsumer.class);

    private final TiConfiguration conf;
    private final TiTableInfo tableInfo;
    private final List<TiRegion> regions;
    private final long startTs;

    // Task local variables
    private List<TiRegion> taskRegions;
    private TiSession session = null;
    private CDCClient client = null;

    private TreeMap<RowKeyWithTs, Row> prewrites = null;
    private TreeMap<RowKeyWithTs, Row> commits = null;

    public FlinkTikvConsumer(
            final TiConfiguration conf,
            final TiTableInfo tableInfo,
            final List<TiRegion> regions,
            final long startTs) {
        this.conf = conf;
        this.tableInfo = tableInfo;
        this.regions = regions;
        this.startTs = startTs;
    }

    @Override
    public void open(final Configuration parameters) throws Exception {
        session = TiSession.create(conf);

        final int numOfTasks = this.getRuntimeContext().getNumberOfParallelSubtasks();
        final int idx = this.getRuntimeContext().getIndexOfThisSubtask();

        final int regionPerTask = (regions.size() / numOfTasks) + ((regions.size() % numOfTasks) > 0 ? 1 : 0);
        if (idx * regionPerTask < regions.size()) {
            taskRegions = regions.subList(
                    idx * regionPerTask,
                    Math.min(idx * regionPerTask + regionPerTask, regions.size())
            );
        } else {
            taskRegions.isEmpty();
        }

        final RegionCDCClientBuilder clientBuilder = new RegionCDCClientBuilder(conf, session.getRegionManager());
        client = new CDCClient(conf, clientBuilder, taskRegions, startTs);

        prewrites = new TreeMap<>();
        commits = new TreeMap<>();
    }


    @Override
    public void run(final SourceContext<RowData> ctx) throws Exception {
        if (taskRegions.isEmpty()) {
            logger.info("No regions to watch, enter idle state");
            ctx.markAsTemporarilyIdle();
        }

        client.start();

        // main loop
        while (!client.isInitialized()) {
            handleRow(client.get());
        }

        long resolvedTs = 0;

        while (true) {
            handleRow(client.get());
            if (client.getMinResolvedTs() > resolvedTs) {
                resolvedTs = client.getMinResolvedTs();
                flushRows(ctx, resolvedTs);
            }
        }
    }

    protected void handleRow(final Row row) {
        if (row == null) return;

        switch (row.getType()) {
            case COMMITTED:
                prewrites.put(RowKeyWithTs.ofStart(row), row);
                // fallthrough
            case COMMIT:
                commits.put(RowKeyWithTs.ofCommit(row), row);
                break;
            case PREWRITE:
                prewrites.put(RowKeyWithTs.ofStart(row), row);
                break;
            case ROLLBACK:
                prewrites.remove(RowKeyWithTs.ofStart(row));
                break;
            default:
                logger.warn("Unsupported row type:" + row.getType());
        }
    }

    protected void flushRows(final SourceContext<RowData> ctx, final long timestamp) {
        while (!commits.isEmpty() && commits.firstKey().timestamp < timestamp) {
            final Row commitRow = commits.pollFirstEntry().getValue();
            final Row prewriteRow = prewrites.remove(RowKeyWithTs.ofStart(commitRow));
            ctx.collect(decodeToRowData(prewriteRow));
        }
        ctx.emitWatermark(new Watermark(timestamp));
    }


    @Override
    public void cancel() {
        // TODO Auto-generated method stub

    }

    @Override
    public void triggerCheckpoint(long checkpointId) throws FlinkException {
        // TODO Auto-generated method stub

    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // TODO Auto-generated method stub
    }

    protected RowData decodeToRowData(final Row row) {
        final RowKey rowKey = RowKey.decode(row.getKey().toByteArray());
        final long handle = rowKey.getHandle();
        switch (row.getOpType()) {
          case DELETE:
            return GenericRowData.ofKind(
                    RowKind.DELETE,
                    TiTableCodec.decodeRow(row.getValue().toByteArray(), handle, tableInfo));
          case PUT:
            return GenericRowData.ofKind(
                    RowKind.INSERT,
                    TiTableCodec.decodeRow(row.getValue().toByteArray(), handle, tableInfo));
          default:
            throw new IllegalArgumentException("Unknown Row Op Type: " + row.getOpType().toString());
        }

    }

    protected static class RowKeyWithTs implements Comparable<RowKeyWithTs> {
        private final long timestamp;
        private final RowKey rowKey;

        private RowKeyWithTs(final long timestamp, final RowKey rowKey) {
            this.timestamp = timestamp;
            this.rowKey = rowKey;
        }

        private RowKeyWithTs(final long timestamp, final byte[] key) {
            this(timestamp, RowKey.decode(key));
        }

        public long getTimestamp() {
          return timestamp;
        }

        public RowKey getRowKey() {
          return rowKey;
        }

        @Override
        public int compareTo(final RowKeyWithTs that) {
            int res = Long.compare(this.timestamp, that.timestamp);
            if (res == 0) {
                res = Long.compare(this.rowKey.getTableId(), that.rowKey.getTableId());
            }
            if (res == 0) {
                res = Long.compare(this.rowKey.getHandle(), that.rowKey.getHandle());
            }
            return res;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.timestamp, this.rowKey.getTableId(), this.rowKey.getHandle());
        }

        @Override
        public boolean equals(final Object thatObj) {
            if (thatObj instanceof RowKeyWithTs) {
                final RowKeyWithTs that = (RowKeyWithTs)thatObj;
                return this.timestamp == that.timestamp && this.rowKey.equals(that.rowKey);
            }
            return false;
        }

        static RowKeyWithTs ofStart(final Row row) {
            return new RowKeyWithTs(row.getStartTs(), row.getKey().toByteArray());
        }

        static RowKeyWithTs ofCommit(final Row row) {
            return new RowKeyWithTs(row.getCommitTs(), row.getKey().toByteArray());
        }
    }
}
