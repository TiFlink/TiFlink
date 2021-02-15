package org.tikv.flink.connectors;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
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
import org.tikv.flink.connectors.coordinators.SnapshotCoordinator;
import org.tikv.flink.connectors.coordinators.Transaction;
import org.tikv.kvproto.Cdcpb.Event.Row;
import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.txn.KVClient;

public class FlinkTikvConsumer extends RichParallelSourceFunction<RowData>
    implements CheckpointListener, CheckpointedFunction, ResultTypeQueryable<RowData> {
  private static final long serialVersionUID = 8647392870748599256L;

  private Logger logger = LoggerFactory.getLogger(FlinkTikvConsumer.class);

  private final TiConfiguration conf;
  private final TiTableInfo tableInfo;
  private final List<TiRegion> regions;
  private final TypeInformation<RowData> typeInfo;

  // Task local variables
  private transient List<TiRegion> taskRegions;
  private transient TiSession session = null;
  private transient CDCClient client = null;
  private transient SnapshotCoordinator coordinator;

  private transient volatile long resolvedTs = 0;
  private transient volatile Transaction currentTransaction = null;

  private transient TreeMap<RowKeyWithTs, Row> prewrites = null;
  private transient TreeMap<RowKeyWithTs, Row> commits = null;

  // Flink State
  private transient ListState<Transaction> resolvedTransactions;

  public FlinkTikvConsumer(
      final TiConfiguration conf,
      final TiTableInfo tableInfo,
      final List<TiRegion> regions,
      final TypeInformation<RowData> typeInfo) {
    this.conf = conf;
    this.tableInfo = tableInfo;
    this.regions = regions;
    this.typeInfo = typeInfo;
  }

  @Override
  public void open(final Configuration config) throws Exception {
    super.open(config);
    session = TiSession.create(conf);

    final int numOfTasks = this.getRuntimeContext().getNumberOfParallelSubtasks();
    final int idx = this.getRuntimeContext().getIndexOfThisSubtask();

    final int regionPerTask =
        (regions.size() / numOfTasks) + ((regions.size() % numOfTasks) > 0 ? 1 : 0);
    if (idx * regionPerTask < regions.size()) {
      taskRegions =
          regions.subList(
              idx * regionPerTask, Math.min(idx * regionPerTask + regionPerTask, regions.size()));
    } else {
      taskRegions = Collections.emptyList();
    }

    final RegionCDCClientBuilder clientBuilder =
        new RegionCDCClientBuilder(conf, session.getRegionManager(), session.getChannelFactory());
    client =
        new CDCClient(
            conf,
            clientBuilder,
            taskRegions,
            resolvedTs > 0 ? resolvedTs : currentTransaction.getStartTs());

    prewrites = new TreeMap<>();
    commits = new TreeMap<>();
  }

  @Override
  public void run(final SourceContext<RowData> ctx) throws Exception {
    if (taskRegions.isEmpty()) {
      logger.info("No regions to watch, enter idle state");
      ctx.markAsTemporarilyIdle();
    }
    // scan original table first
    if (resolvedTs == 0) {
      synchronized (currentTransaction) {
        long startTs = currentTransaction.getStartTs();
        final KVClient scanClient = session.createKVClient();
        for (final TiRegion region : taskRegions) {
          for (final KvPair pair :
              scanClient.scan(region.getStartKey(), region.getEndKey(), startTs)) {
            if (TypeUtils.isRecordKey(pair.getKey().toByteArray())) {
              ctx.collectWithTimestamp(decodeToRowData(pair), startTs);
            }
          }
        }

        resolvedTs = startTs;
        ctx.emitWatermark(new Watermark(resolvedTs));
      }
    }

    // start consuming CDC
    client.start();

    // main loop
    while (!client.isInitialized()) {
      handleRow(client.get());
    }

    while (true) {
      handleRow(client.get());
      if (client.getMinResolvedTs() > resolvedTs + 1_000_000
          && resolvedTs < currentTransaction.getStartTs()) {
        synchronized (currentTransaction) {
          final long nextTs = Math.min(client.getMinResolvedTs(), currentTransaction.getStartTs());
          flushRows(ctx, nextTs);
          resolvedTs = nextTs;
        }
      }
    }
  }

  protected void handleRow(final Row row) {
    logger.debug("handle row: {}", row);
    if (row == null) return;

    if (!TypeUtils.isRecordKey(row.getKey().toByteArray())) {
      // Don't handle index key for now
      return;
    }

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
    logger.info("flush rows: {}", timestamp);
    while (!commits.isEmpty() && commits.firstKey().timestamp < timestamp) {
      final Row commitRow = commits.pollFirstEntry().getValue();
      final Row prewriteRow = prewrites.remove(RowKeyWithTs.ofStart(commitRow));
      final RowData rowData = decodeToRowData(prewriteRow);
      ctx.collectWithTimestamp(rowData, timestamp);
    }
    ctx.emitWatermark(new Watermark(timestamp));
  }

  @Override
  public void cancel() {
    // TODO: implement this

  }

  @Override
  public void snapshotState(final FunctionSnapshotContext context) throws Exception {
    logger.info("snapshotState checkpoint: {}", context.getCheckpointId());
    while (resolvedTs < currentTransaction.getStartTs()) {
      Thread.yield();
    }
    synchronized (currentTransaction) {
      resolvedTransactions.clear();
      resolvedTransactions.add(currentTransaction);
      currentTransaction = coordinator.openTransaction(context.getCheckpointId());
    }
  }

  @Override
  public void initializeState(final FunctionInitializationContext context) throws Exception {
    logger.info("initialize checkpoint");
    resolvedTransactions =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>("resolvedTsState", TransactionSerializer.INSTANCE));
    for (final Transaction ts : resolvedTransactions.get()) {
      resolvedTs = ts.getStartTs();
    }

    currentTransaction = coordinator.openTransaction(0);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    logger.info("checkpoint completed: {}", checkpointId);
  }

  protected RowData decodeToRowData(final Row row) {
    final RowKey rowKey = RowKey.decode(row.getKey().toByteArray());
    final long handle = rowKey.getHandle();
    switch (row.getOpType()) {
      case DELETE:
        return GenericRowData.ofKind(
            RowKind.DELETE,
            TypeUtils.getObjectsWithDataTypes(
                TiTableCodec.decodeKeyOnly(handle, tableInfo), tableInfo));
      case PUT:
        return GenericRowData.ofKind(
            RowKind.INSERT,
            TypeUtils.getObjectsWithDataTypes(
                TiTableCodec.decodeRow(row.getValue().toByteArray(), handle, tableInfo),
                tableInfo));
      default:
        throw new IllegalArgumentException("Unknown Row Op Type: " + row.getOpType().toString());
    }
  }

  protected RowData decodeToRowData(final KvPair kvPair) {
    final RowKey rowKey = RowKey.decode(kvPair.getKey().toByteArray());
    final long handle = rowKey.getHandle();
    return GenericRowData.ofKind(
        RowKind.INSERT,
        TypeUtils.getObjectsWithDataTypes(
            TiTableCodec.decodeRow(kvPair.getValue().toByteArray(), handle, tableInfo), tableInfo));
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
        final RowKeyWithTs that = (RowKeyWithTs) thatObj;
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

  @Override
  public TypeInformation<RowData> getProducedType() {
    return typeInfo;
  }
}
