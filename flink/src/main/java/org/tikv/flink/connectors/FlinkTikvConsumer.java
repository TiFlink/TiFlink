package org.tikv.flink.connectors;

import com.google.common.base.Preconditions;
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
import org.tikv.common.codec.TableCodec;
import org.tikv.common.key.Key;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.util.KeyRangeUtils;
import org.tikv.flink.connectors.coordinator.Coordinator;
import org.tikv.flink.connectors.coordinator.Transaction;
import org.tikv.kvproto.Cdcpb.Event.Row;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.txn.KVClient;

public class FlinkTikvConsumer extends RichParallelSourceFunction<RowData>
    implements CheckpointListener, CheckpointedFunction, ResultTypeQueryable<RowData> {
  private static final long serialVersionUID = 8647392870748599256L;

  private static final Logger LOGGER = LoggerFactory.getLogger(FlinkTikvConsumer.class);

  private final TiConfiguration conf;
  private final TiTableInfo tableInfo;
  private final TypeInformation<RowData> typeInfo;
  private final TransactionHolder txnHolder;
  private final Coordinator coordinator;

  // Task local variables
  private transient TiSession session = null;
  private transient KeyRange keyRange = null;
  private transient CDCClient client = null;
  private transient SourceContext<RowData> sourceContext = null;

  private transient volatile long resolvedTs = 0;

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
    this.txnHolder = new TransactionHolder();
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
    sourceContext = ctx;

    if (resolvedTs == 0) {
      synchronized (sourceContext) {
        scanRows();
      }
    }

    // start consuming CDC
    LOGGER.info("start consuming CDC");
    client.start(resolvedTs);

    pollRows();
  }

  protected void handleRow(final Row row) {
    LOGGER.debug("handle row: {}", row);
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

  protected void scanRows() {
    LOGGER.debug("scan original table");
    long startTs = txnHolder.getStartTs();

    final KVClient scanClient = session.createKVClient();
    synchronized (sourceContext) {
      for (final KvPair pair : scanClient.scan(keyRange.getStart(), keyRange.getEnd(), startTs)) {
        if (TypeUtils.isRecordKey(pair.getKey().toByteArray())) {
          sourceContext.collectWithTimestamp(decodeToRowData(pair), startTs);
        }
      }
      sourceContext.emitWatermark(new Watermark(startTs));
    }

    resolvedTs = startTs;
  }

  protected void pollRows() throws Exception {
    LOGGER.debug("poll rows: {}, resolvedTs:{}", commits.size(), resolvedTs);
    while (resolvedTs >= 0) {
      for (int i = 0; i < 1000; i++) {
        final Row row = client.get();
        if (row == null) {
          break;
        }
        handleRow(row);
      }
      resolvedTs = client.getMinResolvedTs();
      if (commits.size() > 50000) {
        flushRows(txnHolder.getStartTs(), false);
      }
    }
  }

  protected void flushRows(final long timestamp, final boolean emitWatermark) {
    Preconditions.checkState(sourceContext != null, "sourceContext shouldn't be null");
    synchronized (sourceContext) {
      while (!commits.isEmpty() && commits.firstKey().timestamp <= timestamp) {
        final Row commitRow = commits.pollFirstEntry().getValue();
        final Row prewriteRow = prewrites.remove(RowKeyWithTs.ofStart(commitRow));
        final RowData rowData = decodeToRowData(prewriteRow);
        sourceContext.collectWithTimestamp(rowData, timestamp);
      }
      if (emitWatermark) {
        sourceContext.emitWatermark(new Watermark(timestamp));
      }
    }
  }

  @Override
  public void cancel() {
    // TODO: abort pending transactions
    try {
      resolvedTs = -1;
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
    final Transaction txn = txnHolder.get();

    while (resolvedTs < txn.getStartTs()) {
      Thread.yield();
    }

    flushRows(txn.getStartTs(), true);

    resolvedTransactions.clear();
    resolvedTransactions.add(txn);
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

    txnHolder.set(coordinator.openTransaction(0));
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    LOGGER.debug("checkpoint completed: {}", checkpointId);
    coordinator.commitTransaction(txnHolder.getCheckpointId());
    txnHolder.set(coordinator.openTransaction(checkpointId));
  }

  protected RowData decodeToRowData(final Row row) {
    final RowKey rowKey = RowKey.decode(row.getKey().toByteArray());
    final long handle = rowKey.getHandle();
    switch (row.getOpType()) {
      case DELETE:
        return GenericRowData.ofKind(
            RowKind.DELETE,
            TypeUtils.getObjectsWithDataTypes(
                TableCodec.decodeObjects(row.getOldValue().toByteArray(), handle, tableInfo),
                tableInfo));
      case PUT:
        try {
          return GenericRowData.ofKind(
              RowKind.INSERT,
              TypeUtils.getObjectsWithDataTypes(
                  TableCodec.decodeObjects(row.getValue().toByteArray(), handle, tableInfo),
                  tableInfo));
        } catch (final RuntimeException e) {
          LOGGER.error("error, row: {}, table: {}", row, tableInfo.getId());
          throw e;
        }
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
            TableCodec.decodeObjects(kvPair.getValue().toByteArray(), handle, tableInfo),
            tableInfo));
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
