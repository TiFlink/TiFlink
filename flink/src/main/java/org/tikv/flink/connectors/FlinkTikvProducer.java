package org.tikv.flink.connectors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import com.google.common.base.Preconditions;
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
import org.tikv.flink.connectors.coordinator.Coordinator;
import org.tikv.flink.connectors.coordinator.Transaction;
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
  private final AtomicBoolean txnCommitting;

  private transient TiSession session = null;
  private transient volatile TransactionHolder txnHolder;
  private transient List<BytePairWrapper> cachedValues;
  private transient Map<Long, TransactionHolder> prewrittenTxnHolders;

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
    this.txnCommitting = new AtomicBoolean();
  }

  @Override
  public void open(final Configuration config) throws Exception {
    LOGGER.info("open sink");
    super.open(config);
    session = TiSession.create(conf);
    cachedValues = new ArrayList<>();
    prewrittenTxnHolders = new HashMap<>();
    txnCommitting.set(false);
  }

  @Override
  public void invoke(final RowData row, final Context context) throws Exception {
    if (txnHolder.get().isNew() || Objects.isNull(txnHolder.getCommitter())) {
      final Transaction newTxn =
          coordinator.prewriteTransaction(txnHolder.get().getCheckpointId(), tableInfo.getId());
      txnHolder = new TransactionHolder(newTxn, createCommitter(newTxn));
    }

    cachedValues.add(encodeRow(row));

    if (cachedValues.size() >= conf.getScanBatchSize()) {
      flushCachedValues();
    }
  }

  private void flushCachedValues() {
    prewrite(txnHolder.get(), txnHolder.getCommitter(), cachedValues);
    txnHolder.addSecondaryKeys(
        () -> cachedValues.stream().map(BytePairWrapper::getKey).map(ByteWrapper::new).iterator());
    cachedValues.clear();
  }

  protected TwoPhaseCommitter createCommitter(final Transaction txn) {
    return new TwoPhaseCommitter(session, txn.getStartTs());
  }

  protected void prewrite(
      final Transaction txn,
      final TwoPhaseCommitter committer,
      final Iterable<BytePairWrapper> values) {
    Preconditions.checkState(txn.isPrewriting(), "Transaction must be prewriting");
    Preconditions.checkNotNull(committer, "Committer can't be null");
    committer.prewriteSecondaryKeys(txn.getPrimaryKey(), values.iterator(), 200);
  }

  protected void commitSecondaryKeys(
      final Transaction txn, final TwoPhaseCommitter committer, final Iterable<ByteWrapper> keys) {
    Preconditions.checkState(txn.isCommitted(), "Transaction must be committed");
    committer.commitSecondaryKeys(keys.iterator(), txn.getCommitTs(), 200);
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
    final TransactionHolder holder = prewrittenTxnHolders.remove(checkpointId);
    if (holder != null) {
      final Transaction txn = coordinator.commitTransaction(holder.get().getCheckpointId());

      if (holder.hasSecondaryKeys()) {
        LOGGER.info("commit secondary keys, size: {}, txn: {}", holder.secondaryKeys.size(), txn);
        final TwoPhaseCommitter committer =
            Objects.isNull(holder.getCommitter()) ? createCommitter(txn) : holder.getCommitter();
        commitSecondaryKeys(txn, committer, holder.getSecondaryKeys());
      }
    }
  }

  @Override
  public void snapshotState(final FunctionSnapshotContext context) throws Exception {
    if (!cachedValues.isEmpty()) {
      flushCachedValues();
    }

    transactionState.clear();
    transactionState.add(txnHolder.get());
    prewrittenTxnHolders.put(context.getCheckpointId(), txnHolder);

    txnHolder = new TransactionHolder(coordinator.openTransaction(context.getCheckpointId()));
    transactionState.add(txnHolder.get());
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

    txnHolder = new TransactionHolder(coordinator.openTransaction(0));
    transactionState.add(txnHolder.get());
  }

  static class TransactionHolder implements AutoCloseable {
    private final Transaction transaction;
    private final TwoPhaseCommitter committer;
    private final List<ByteWrapper> secondaryKeys = new ArrayList<>(); // TODO: use offheap buffer

    TransactionHolder(final Transaction txn, final TwoPhaseCommitter committer) {
      this.transaction = txn;
      this.committer = committer;
    }

    TransactionHolder(final Transaction txn) {
      this(txn, null);
    }

    void addSecondaryKey(final ByteWrapper key) {
      secondaryKeys.add(key);
    }

    void addSecondaryKeys(final Iterable<ByteWrapper> keys) {
      for (final ByteWrapper key : keys) {
        addSecondaryKey(key);
      }
    }

    Transaction get() {
      return transaction;
    }

    TwoPhaseCommitter getCommitter() {
      return committer;
    }

    boolean hasSecondaryKeys() {
      return !secondaryKeys.isEmpty();
    }

    Iterable<ByteWrapper> getSecondaryKeys() {
      return secondaryKeys;
    }

    @Override
    public void close() throws Exception {
      if (Objects.nonNull(committer)) {
        committer.close();
      }
    }

    @Override
    protected void finalize() throws Throwable {
      close();
    }
  }
}
