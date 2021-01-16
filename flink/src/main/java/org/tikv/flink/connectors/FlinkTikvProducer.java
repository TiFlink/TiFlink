package org.tikv.flink.connectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
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
import org.tikv.common.codec.TiTableCodec;
import org.tikv.common.exception.TiBatchWriteException;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.txn.TwoPhaseCommitter;
import shade.com.google.common.base.Preconditions;

public class FlinkTikvProducer
        extends TwoPhaseCommitSinkFunction<
                RowData,
                FlinkTikvProducer.TiKVTransactionState,
                FlinkTikvProducer.TiKVTransactionContext> {

    private static final long serialVersionUID = 1L;

    private Logger logger = LoggerFactory.getLogger(FlinkTikvProducer.class);

    private final TiConfiguration conf;
    private final TiTableInfo tableInfo;
    private final FieldGetter[] fieldGetters;;
    private final int pkIndex;

    private TiSession session = null;

    public FlinkTikvProducer(
            final TiConfiguration conf,
            final TiTableInfo tableInfo,
            final DataType dataType
        ) {
        super(new TransactionStateSerializer(), new ContextStateSerializer());
        this.conf = conf;
        this.tableInfo = tableInfo;

        final List<LogicalType> colTypes = dataType.getLogicalType().getChildren();
        fieldGetters = new FieldGetter[colTypes.size()];
        for (int i = 0; i < fieldGetters.length; i++) {
            fieldGetters[i] = TypeUtils.createFieldGetter(colTypes.get(i), i);
        }
        logger.info("colTypes: {}", colTypes);

        Optional<TiColumnInfo> pk =
            tableInfo.getColumns().stream().filter(TiColumnInfo::isPrimaryKey).findFirst();
        Preconditions.checkArgument(pk.isPresent() && TypeUtils.isIntType(pk.get().getType())); 

        this.pkIndex = tableInfo.getColumns().indexOf(pk.get());
    }

    private TiSession getSession() {
        if (session == null) {
            session = TiSession.create(conf);
        }
        return session;
    }

    public static class TiKVTransactionState {
        private static final int MAX_BUFFER_SIZE = 1 << 20;

        private final transient TwoPhaseCommitter twoPhaseCommitter;
        private final transient List<BytePairWrapper> buffer;
        private final transient List<ByteWrapper> secondaryKeys;

        private Long commitTs = null;
        private byte[] pk = null;

        protected TiKVTransactionState(final TwoPhaseCommitter twoPhaseCommitter, final Long commitTs, final byte[] pk) {
            this.twoPhaseCommitter = twoPhaseCommitter;
            this.commitTs = commitTs;
            this.pk = pk == null ? null : Arrays.copyOf(pk, pk.length);
            buffer = new ArrayList<>(1<<15);
            secondaryKeys = new ArrayList<>(1<<15);
        }

        public TiKVTransactionState(final TwoPhaseCommitter twoPhaseCommitter) {
            this(twoPhaseCommitter, null, null);
        }

        public void write(final byte[] key, final byte[] value) {
            if (pk == null){
                pk = Arrays.copyOf(key, key.length);
                twoPhaseCommitter.prewritePrimaryKey(ConcreteBackOffer.newRawKVBackOff(), pk, value);
            } else {
                if (buffer.size() >= MAX_BUFFER_SIZE) {
                    twoPhaseCommitter.prewriteSecondaryKeys(pk, buffer.iterator(), 100);
                    buffer.clear();
                }
                buffer.add(new BytePairWrapper(key, value));
                secondaryKeys.add(new ByteWrapper(key));
            }
        }

        public void precommit(final long commitTs) {
            if (pk == null) return;

            if (!buffer.isEmpty()) {
                twoPhaseCommitter.prewriteSecondaryKeys(pk, buffer.iterator(), 100);
            }

            twoPhaseCommitter.commitPrimaryKey(ConcreteBackOffer.newRawKVBackOff(), pk, commitTs);
            this.commitTs = commitTs;
        }

        public void commit() {
            if (pk == null) return;
            twoPhaseCommitter.commitSecondaryKeys(secondaryKeys.iterator(), commitTs, 100);
        }
    }

    public static class TiKVTransactionContext {
        final Set<Long> tsSet;

        public TiKVTransactionContext(Set<Long> tsSet) {
            checkNotNull(tsSet);
            this.tsSet = tsSet;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TiKVTransactionContext that = (TiKVTransactionContext) o;
            return tsSet.equals(that.tsSet);
        }

        @Override
        public int hashCode() {
            return tsSet.hashCode();
        }
    }

    @Override
    public void open(final Configuration config) throws Exception {
        logger.info("open sink");
        getSession();

        super.open(config);
    }

    @Override
    public void invoke(final TiKVTransactionState transaction, final RowData row, final Context context) throws TiBatchWriteException {
        final Object pkValue = fieldGetters[pkIndex].getFieldOrNull(row);
        long handle = 0;
        if (pkValue instanceof Long) {
            handle = ((Long)pkValue).longValue();
        } else {
            handle = ((Integer)pkValue).longValue();
        }
        final RowKey rowKey = RowKey.toRowKey(tableInfo.getId(), handle);
        if (row.getRowKind() == RowKind.DELETE) {
            transaction.write(rowKey.getBytes(), new byte[0]);
        } else {
            try {
                transaction.write(
                    rowKey.getBytes(),
                    TiTableCodec.encodeRow(tableInfo.getColumns(), TypeUtils.toObjects(row, fieldGetters), true, true)
                );
            } catch (final Throwable t) {
                logger.error("failed to write value", t);
                throw new RuntimeException(t);
            }
        }
    }

    @Override
    public void close() throws Exception {
        session.close();
    }

    @Override
    protected TiKVTransactionState beginTransaction() {
        logger.info("begin transaction");
        final long startTs = getSession().getTimestamp().getVersion();
        final TwoPhaseCommitter twoPhaseCommitter = new TwoPhaseCommitter(conf, startTs);
        return new TiKVTransactionState(twoPhaseCommitter);
    }

    @Override
    protected void preCommit(final TiKVTransactionState transaction) {
        logger.info("precommit");
        transaction.precommit(session.getTimestamp().getVersion());
    }

    @Override
    protected void commit(final TiKVTransactionState transaction) throws TiBatchWriteException {
        logger.info("commit");
        transaction.commit();
    }

    @Override
    protected void abort(TiKVTransactionState transaction) {
    }

    @Internal
    public static class TransactionStateSerializer
            extends TypeSerializerSingleton<TiKVTransactionState> {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TiKVTransactionState createInstance() {
            return null;
        }

        @Override
        public TiKVTransactionState copy(TiKVTransactionState from) {
            return from;
        }

        @Override
        public TiKVTransactionState copy(TiKVTransactionState from, TiKVTransactionState reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(TiKVTransactionState record, DataOutputView target)
                throws IOException {
            if (record.commitTs == null) {
                target.writeBoolean(false);
            } else {
                target.writeBoolean(true);
                target.writeLong(record.commitTs);
            }
            if (record.pk == null) {
                target.writeBoolean(false);
            } else {
                target.writeBoolean(true);
                target.write(record.pk);
            }
        }

        @Override
        public TiKVTransactionState deserialize(final DataInputView source) throws IOException {
            Long ts = null;
            byte[] key = new byte[1024];
            if (source.readBoolean()) {
                ts = source.readLong();
            }
            if (source.readBoolean()) {
                final int len = source.read(key);
                if (len > 0) {
                    return new TiKVTransactionState(null, ts, Arrays.copyOf(key, len));
                }
            }

            return new TiKVTransactionState(null, ts, null);
        }

        @Override
        public TiKVTransactionState deserialize(TiKVTransactionState reuse, DataInputView source)
                throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeLong(source.readLong());
            target.write(source.readUTF().getBytes());
        }

        // -----------------------------------------------------------------------------------

        @Override
        public TypeSerializerSnapshot<TiKVTransactionState> snapshotConfiguration() {
            return new TransactionStateSerializerSnapshot();
        }

        /** Serializer configuration snapshot for compatibility and format evolution. */
        @SuppressWarnings("WeakerAccess")
        public static final class TransactionStateSerializerSnapshot
                extends SimpleTypeSerializerSnapshot<TiKVTransactionState> {

            public TransactionStateSerializerSnapshot() {
                super(TransactionStateSerializer::new);
            }
        }
    }

    @Internal
    public static class ContextStateSerializer
            extends TypeSerializerSingleton<TiKVTransactionContext> {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TiKVTransactionContext createInstance() {
            return null;
        }

        @Override
        public TiKVTransactionContext copy(TiKVTransactionContext from) {
            return from;
        }

        @Override
        public TiKVTransactionContext copy(
                TiKVTransactionContext from, TiKVTransactionContext reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(TiKVTransactionContext record, DataOutputView target)
                throws IOException {
            int numIds = record.tsSet.size();
            target.writeInt(numIds);
            for (Long ts : record.tsSet) {
                target.writeLong(ts);
            }
        }

        @Override
        public TiKVTransactionContext deserialize(DataInputView source) throws IOException {
            int numIds = source.readInt();
            Set<Long> tsSet = new HashSet<>(numIds);
            for (int i = 0; i < numIds; i++) {
                tsSet.add(source.readLong());
            }
            return new TiKVTransactionContext(tsSet);
        }

        @Override
        public TiKVTransactionContext deserialize(
                TiKVTransactionContext reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            int numIds = source.readInt();
            target.writeInt(numIds);
            for (int i = 0; i < numIds; i++) {
                target.writeLong(source.readLong());
            }
        }

        // -----------------------------------------------------------------------------------

        @Override
        public TypeSerializerSnapshot<TiKVTransactionContext> snapshotConfiguration() {
            return new ContextStateSerializerSnapshot();
        }

        /** Serializer configuration snapshot for compatibility and format evolution. */
        @SuppressWarnings("WeakerAccess")
        public static final class ContextStateSerializerSnapshot
                extends SimpleTypeSerializerSnapshot<TiKVTransactionContext> {

            public ContextStateSerializerSnapshot() {
                super(ContextStateSerializer::new);
            }
        }
    }
}

