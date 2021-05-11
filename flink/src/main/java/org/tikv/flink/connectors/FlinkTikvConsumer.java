package org.tikv.flink.connectors;

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
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.codec.TiTableCodec;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.flink.connectors.coordinator.Coordinator;
import org.tikv.flink.connectors.coordinator.Transaction;
import org.tikv.kvproto.Cdcpb.Event.Row;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.txn.KVClient;

public class FlinkTikvConsumer extends RichParallelSourceFunction<RowData>
    implements CheckpointListener, CheckpointedFunction, ResultTypeQueryable<RowData> {
  private static final long serialVersionUID = 8647392870748599256L;

  private static final long PUNCTUATE_DURATOIN = 1000_000_000;

  private static final Logger LOGGER = LoggerFactory.getLogger(FlinkTikvConsumer.class);

  private final TiConfiguration conf;
  private final TiTableInfo tableInfo;
  private final TypeInformation<RowData> typeInfo;
  private final Coordinator coordinator;

  // Task local variables
  private transient TiSession session = null;
  private transient KeyRange keyRange = null;
  private transient CDCClient client = null;

  private transient volatile long resolvedTs = 0;
  private transient volatile Transaction currentTransaction = null;

  private transient TreeMap<RowKeyWithTs, Row> prewrites = null;
  private transient TreeMap<RowKeyWithTs, Row> commits = null;

  // Flink State
  private transient ListState<Transaction> resolvedTransactions;

  public FlinkTikvConsumer(
      final TiConfiguration conf,
      final TiTableInfo tableInfo,
      final TypeInformation<RowData> typeInfo,
      final Coordinator coordinator) {
    this.conf = conf;
    this.tableInfo = tableInfo;
    this.typeInfo = typeInfo;
    this.coordinator = coordinator;
  }

  @Override
  public void open(final Configuration config) throws Exception {
    super.open(config);
    session = TiSession.create(conf);
    keyRange =
        TableKeyRangeUtils.getTableKeyRange(
            tableInfo.getId(),
            this.getRuntimeContext().getNumberOfParallelSubtasks(),
            this.getRuntimeContext().getIndexOfThisSubtask());
    client = new CDCClient(session, keyRange);

    prewrites = new TreeMap<>();
    commits = new TreeMap<>();
  }

  @Override
  public void run(final SourceContext<RowData> ctx) throws Exception {
    LOGGER.info(
        "running (thread: {}, tasks: {}/{}, ctx: {})",
        Thread.currentThread().getId(),
        this.getRuntimeContext().getIndexOfThisSubtask(),
        this.getRuntimeContext().getNumberOfParallelSubtasks(),
        ctx.getClass().getTypeName());

    final Object checkpointLock = ctx.getCheckpointLock();

    synchronized (checkpointLock) {
      // scan original table first
      if (resolvedTs == 0) {
        scanRows(ctx);
      }

      // start consuming CDC
      LOGGER.info("start consuming CDC");
      client.start(resolvedTs);

      pollRows(ctx); // pull rows for the first transaction
    }

    while (true) {
      while (resolvedTs == currentTransaction.getStartTs()) {
        Thread.yield();
      }

      synchronized (checkpointLock) {
        pollRows(ctx);
      }
    }
  }

  protected void handleRow(final Row row) {
    LOGGER.debug("handle row: {}", row);
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
        LOGGER.warn("Unsupported row type:" + row.getType());
    }
  }

  protected void scanRows(final SourceContext<RowData> ctx) {
    LOGGER.debug("scan original table");
    synchronized (currentTransaction) {
      long startTs = currentTransaction.getStartTs();
      final KVClient scanClient = session.createKVClient();
      for (final KvPair pair : scanClient.scan(keyRange.getStart(), keyRange.getEnd(), startTs)) {
        if (TypeUtils.isRecordKey(pair.getKey().toByteArray())) {
          ctx.collectWithTimestamp(decodeToRowData(pair), startTs);
        }
      }

      resolvedTs = startTs;
    }
  }

  protected void pollRows(final SourceContext<RowData> ctx) throws Exception {
    synchronized (currentTransaction) {
      while (resolvedTs < currentTransaction.getStartTs()) {
        LOGGER.debug("poll rows: {}, resolvedTs:{}", commits.size(), resolvedTs);
        handleRow(client.get());
        if (resolvedTs + PUNCTUATE_DURATOIN <= client.getMinResolvedTs()
            || currentTransaction.getStartTs() <= client.getMinResolvedTs()) {
          final long nextTs = Math.min(client.getMinResolvedTs(), currentTransaction.getStartTs());
          flushRows(ctx, nextTs);
          resolvedTs = nextTs;
        }
      }
    }
  }

  protected void flushRows(final SourceContext<RowData> ctx, final long timestamp) {
    LOGGER.info(
        "flush rows: {}, timestamp: {}, thread id: {}",
        commits.size(),
        timestamp,
        Thread.currentThread().getId());
    while (!commits.isEmpty() && commits.firstKey().timestamp <= timestamp) {
      final Row commitRow = commits.pollFirstEntry().getValue();
      final Row prewriteRow = prewrites.remove(RowKeyWithTs.ofStart(commitRow));
      final RowData rowData = decodeToRowData(prewriteRow);
      ctx.collectWithTimestamp(rowData, timestamp);
    }
    ctx.emitWatermark(new Watermark(timestamp));
  }

  @Override
  public void cancel() {
    // TODO: abort pending transactions
    try {
      coordinator.close();
    } catch (final Exception e) {
      LOGGER.error("Unable to close coordinator", e);
    }
  }

  @Override
  public void snapshotState(final FunctionSnapshotContext context) throws Exception {
    LOGGER.info(
        "snapshotState checkpoint: {}, thread: {}",
        context.getCheckpointId(),
        Thread.currentThread().getId());
    synchronized (currentTransaction) {
      resolvedTransactions.clear();
      resolvedTransactions.add(currentTransaction);
      currentTransaction = coordinator.openTransaction(context.getCheckpointId());
    }
  }

  @Override
  public void initializeState(final FunctionInitializationContext context) throws Exception {
    LOGGER.info("initialize checkpoint");
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
    LOGGER.info("checkpoint completed: {}", checkpointId);
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
