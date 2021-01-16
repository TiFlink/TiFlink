package org.apache.flink.connector.tikv.sink;

import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.TwoPhaseCommitter;
import com.pingcap.tikv.exception.TiBatchWriteException;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class FlinkTiKVProducer<IN>
        extends TwoPhaseCommitSinkFunction<
                IN,
                FlinkTiKVProducer.TiKVTransactionState,
                FlinkTiKVProducer.TiKVTransactionContext> {

    private final TiConfiguration configuration;
    private final TiKVSerializationSchema<IN> schema;
    private final TiSession session;

    public FlinkTiKVProducer(TiConfiguration tiConfiguration, TiKVSerializationSchema<IN> schema) {
        super(new TransactionStateSerializer(), new ContextStateSerializer());
        this.configuration = tiConfiguration;
        this.schema = schema;
        this.session = TiSession.getInstance(tiConfiguration);
    }

    public static class TiKVTransactionState {

        private final transient TwoPhaseCommitter twoPhaseCommitter;
        private byte[] key;
        private Long ts;

        public TiKVTransactionState(TwoPhaseCommitter twoPhaseCommitter, long ts) {
            this.twoPhaseCommitter = twoPhaseCommitter;
            this.ts = ts;
        }

        public TiKVTransactionState(TwoPhaseCommitter twoPhaseCommitter, byte[] key, Long ts) {
            this.twoPhaseCommitter = twoPhaseCommitter;
            this.key = key;
            this.ts = ts;
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
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
    }

    @Override
    public void invoke(TiKVTransactionState transaction, IN next, Context context)
            throws TiBatchWriteException {
        Tuple2<byte[], byte[]> tuple = schema.serialize(next);
        byte[] key = tuple.getField(0);
        byte[] value = tuple.getField(1);
        transaction.twoPhaseCommitter.prewritePrimaryKey(
                ConcreteBackOffer.newCustomBackOff(BackOffer.PREWRITE_MAX_BACKOFF), key, value);
        transaction.key = key;
        transaction.ts = session.getTimestamp().getVersion();
    }

    @Override
    public void close() throws Exception {
        session.close();
    }

    @Override
    protected TiKVTransactionState beginTransaction() {
        final long startTime = session.getTimestamp().getVersion();
        TwoPhaseCommitter twoPhaseCommitter = new TwoPhaseCommitter(configuration, startTime);
        return new TiKVTransactionState(twoPhaseCommitter, startTime);
    }

    @Override
    protected void preCommit(TiKVTransactionState transaction) {}

    @Override
    protected void commit(TiKVTransactionState transaction) throws TiBatchWriteException {
        transaction.twoPhaseCommitter.commitPrimaryKey(
                ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_COMMIT_BACKOFF),
                transaction.key,
                transaction.ts);
    }

    @Override
    protected void abort(TiKVTransactionState transaction) {}

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
            if (record.ts == null) {
                target.writeBoolean(false);
            } else {
                target.writeBoolean(true);
                target.writeLong(record.ts);
            }
            target.write(record.key);
        }

        @Override
        public TiKVTransactionState deserialize(DataInputView source) throws IOException {
            Long ts = null;
            if (source.readBoolean()) {
                ts = source.readLong();
            }
            byte[] key = source.readUTF().getBytes();
            return new TiKVTransactionState(null, key, ts);
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
