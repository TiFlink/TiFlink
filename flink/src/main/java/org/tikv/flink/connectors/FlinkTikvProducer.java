package org.tikv.flink.connectors;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.ByteWrapper;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.codec.TableCodec;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.flink.TypeUtils;
import org.tikv.flink.coordinator.Coordinator;
import org.tikv.flink.coordinator.Transaction;
import org.tikv.txn.TwoPhaseCommitter;

public class FlinkTikvProducer extends RichSinkFunction<RowData>
    implements CheckpointedFunction, CheckpointListener {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(FlinkTikvProducer.class);

  private final TiConfiguration conf;
  private final TiTableInfo tableInfo;
  private final FieldGetter[] fieldGetters;
  private final int pkIndex;
  private final Coordinator coordinator;
  private final TransactionHolder txnHolder;

  private transient TiSession session = null;
  private transient List<BytePairWrapper> cachedValues;
  private transient Map<Long, CommitContext> commitContextMap;
  private transient AtomicBoolean flushBlockingFlag;

  // transactions
  protected transient ListState<Transaction> transactionState;

  public FlinkTikvProducer(
      final TiConfiguration conf,
      final TiTableInfo tableInfo,
      final DataType dataType,
      final Coordinator coordinator) {
    this.conf = conf;
    this.tableInfo = tableInfo;
    this.coordinator = coordinator;
    Preconditions.checkNotNull(coordinator, "coordinator can't be null");

    final List<LogicalType> colTypes = dataType.getLogicalType().getChildren();
    fieldGetters = new FieldGetter[colTypes.size()];
    for (int i = 0; i < fieldGetters.length; i++) {
      fieldGetters[i] = TypeUtils.createFieldGetter(colTypes.get(i), i);
    }
    LOGGER.info("colTypes: {}", colTypes);

    Optional<TiColumnInfo> pk =
        tableInfo.getColumns().stream().filter(TiColumnInfo::isPrimaryKey).findFirst();
    Preconditions.checkArgument(pk.isPresent() && TypeUtils.isIntType(pk.get().getType()));

    this.pkIndex = tableInfo.getColumns().indexOf(pk.get());
    this.txnHolder = new TransactionHolder();
  }

  @Override
  public void open(final Configuration config) throws Exception {
    LOGGER.info("open sink");
    super.open(config);
    session = TiSession.create(conf);
    cachedValues = new ArrayList<>();
    commitContextMap = new ConcurrentHashMap<>();
    flushBlockingFlag = new AtomicBoolean(false);
  }

  @Override
  public void invoke(final RowData row, final Context context) throws Exception {
    cachedValues.add(encodeRow(row));

    if (cachedValues.size() >= conf.getScanBatchSize()) {
      flushCachedValues();
    }
  }

  private void flushCachedValues() {
    if (cachedValues.isEmpty()) return;

    while (flushBlockingFlag.getAcquire()) {
      Thread.yield();
    }

    synchronized (txnHolder) {
      if (txnHolder.isNew()) {
        txnHolder.set(
            coordinator.prewriteTransaction(txnHolder.getCheckpointId(), tableInfo.getId()));
      }
      final Transaction txn = txnHolder.get();
      final CommitContext ctx = getCommitContext(txn);
      synchronized (ctx) {
        prewrite(txn, ctx.getCommitter(), cachedValues.iterator());
        for (final BytePairWrapper pair : cachedValues) {
          ctx.addSecondaryKey(pair.getKey());
        }
      }
    }
    cachedValues.clear();
  }

  protected CommitContext getCommitContext(final Transaction txn) {
    final TiSession sess = session;
    final long startTs = txn.getStartTs();

    return commitContextMap.computeIfAbsent(
        txn.getCheckpointId(),
        (key) -> {
          final TwoPhaseCommitter committer = new TwoPhaseCommitter(sess, startTs);
          return new CommitContext(committer);
        });
  }

  protected void prewrite(
      final Transaction txn,
      final TwoPhaseCommitter committer,
      final Iterator<BytePairWrapper> valueIter) {
    Preconditions.checkState(txn.isPrewriting(), "Transaction must be prewriting");
    Preconditions.checkNotNull(committer, "Committer can't be null");
    committer.prewriteSecondaryKeys(txn.getPrimaryKey(), valueIter, 200);
  }

  protected void commitSecondaryKeys(
      final Transaction txn, final TwoPhaseCommitter committer, final Iterator<ByteWrapper> keys) {
    Preconditions.checkState(txn.isCommitted(), "Transaction must be committed");
    committer.commitSecondaryKeys(keys, txn.getCommitTs(), 200);
  }

  public BytePairWrapper encodeRow(final RowData row) {
    final Object pkValue = fieldGetters[pkIndex].getFieldOrNull(row);
    long handle = 0;
    if (pkValue instanceof Long) {
      handle = ((Long) pkValue).longValue();
    } else {
      handle = ((Integer) pkValue).longValue();
    }
    final RowKey rowKey = RowKey.toRowKey(tableInfo.getId(), handle);
    if (row.getRowKind() == RowKind.DELETE) {
      return new BytePairWrapper(rowKey.getBytes(), new byte[0]);
    } else {
      try {
        return new BytePairWrapper(
            rowKey.getBytes(),
            TableCodec.encodeRow(
                tableInfo.getColumns(), TypeUtils.toObjects(row, fieldGetters), true, true));
      } catch (final Throwable t) {
        LOGGER.error("failed to encode row", t);
        throw new RuntimeException(t);
      }
    }
  }

  @Override
  public void close() throws Exception {
    coordinator.close();
  }

  @Override
  public void notifyCheckpointComplete(final long checkpointId) throws Exception {
    LOGGER.info("checkpoint complete, checkpointId: {}", checkpointId);

    final Transaction txn = coordinator.commitTransaction(txnHolder.getCheckpointId());
    final CommitContext ctx = commitContextMap.remove(txn.getCheckpointId());

    // start new transaction
    txnHolder.set(coordinator.openTransaction(checkpointId));
    flushBlockingFlag.setRelease(false);

    if (ctx != null) {
      commitSecondaryKeys(txn, ctx.getCommitter(), ctx.secondaryKeyIter());
    }
  }

  @Override
  public void snapshotState(final FunctionSnapshotContext context) throws Exception {
    LOGGER.info("snapshotState, checkpointId: {}", context.getCheckpointId());
    flushCachedValues();

    transactionState.clear();
    transactionState.add(txnHolder.get());

    flushBlockingFlag.setRelease(true);
  }

  @Override
  public void initializeState(final FunctionInitializationContext context) throws Exception {
    transactionState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>("transactionState", TransactionSerializer.INSTANCE));

    for (final Transaction txn : transactionState.get()) {
      if (txn.isNew()) {
        coordinator.abortTransaction(txn.getCheckpointId());
      } else {
        coordinator.commitTransaction(txn.getCheckpointId());
      }
    }
    transactionState.clear();

    txnHolder.set(coordinator.openTransaction(0));
    transactionState.add(txnHolder.get());
  }

  static class CommitContext {
    private final TwoPhaseCommitter committer;
    // TODO: use off heap buffer
    private final List<ByteWrapper> secondaryKeys;

    CommitContext(final TwoPhaseCommitter committer) {
      this.committer = committer;
      secondaryKeys = new ArrayList<>();
    }

    public TwoPhaseCommitter getCommitter() {
      return committer;
    }

    public Iterator<ByteWrapper> secondaryKeyIter() {
      return secondaryKeys.iterator();
    }

    public void addSecondaryKey(final byte[] key) {
      secondaryKeys.add(new ByteWrapper(key));
    }

    @Override
    protected void finalize() throws Throwable {
      committer.close();
    }
  }
}
