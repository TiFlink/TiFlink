package org.tikv.flink.connectors;

import java.io.IOException;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.tikv.flink.connectors.coordinators.ImmutableTransaction;
import org.tikv.flink.connectors.coordinators.Transaction;

public class TransactionSerializer extends TypeSerializer<Transaction> {
  private static final long serialVersionUID = 6896453170029198335L;

  public static final TransactionSerializer INSTANCE = new TransactionSerializer();

  @Override
  public boolean isImmutableType() {
    return true;
  }

  @Override
  public TypeSerializer<Transaction> duplicate() {
    return INSTANCE;
  }

  @Override
  public Transaction createInstance() {
    return ImmutableTransaction.builder().build();
  }

  @Override
  public Transaction copy(final Transaction from) {
    return from;
  }

  @Override
  public Transaction copy(final Transaction from, final Transaction reuse) {
    return from;
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(final Transaction record, final DataOutputView target) throws IOException {
    target.writeLong(record.getCheckpointId());
    target.writeLong(record.getStartTs());
    target.writeLong(record.getCommitTs());
    writePrimaryKey(record.getPrimaryKey(), target);
    target.writeUTF(record.getStatus().toString());
  }

  @Override
  public Transaction deserialize(final DataInputView source) throws IOException {
    return ImmutableTransaction.builder()
        .checkpointId(source.readLong())
        .startTs(source.readLong())
        .commitTs(source.readLong())
        .primaryKey(readPrimaryKey(source))
        .status(Transaction.Status.valueOf(source.readUTF()))
        .build();
  }

  @Override
  public Transaction deserialize(final Transaction reuse, final DataInputView source)
      throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(final DataInputView source, final DataOutputView target) throws IOException {
    target.writeLong(source.readLong()); // checkpointId
    target.writeLong(source.readLong()); // startTs
    target.writeLong(source.readLong()); // commitTs
    writePrimaryKey(readPrimaryKey(source), target);
    target.writeUTF(source.readUTF());
  }

  protected byte[] readPrimaryKey(final DataInputView source) throws IOException {
    final byte[] res = new byte[source.readInt()];
    if (res.length > 0) {
      source.readFully(res);
    }
    return res;
  }

  protected void writePrimaryKey(final byte[] pk, final DataOutputView output) throws IOException {
    final int pkLen = (pk == null) ? 0 : pk.length;
    output.writeInt(pkLen);
    if (pkLen > 0) {
      output.write(pk);
    }
  }

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof Transaction;
  }

  @Override
  public int hashCode() {
    return 42;
  }

  @Override
  public TypeSerializerSnapshot<Transaction> snapshotConfiguration() {
    return new TransactionSerializerSnapshot();
  }

  public static class TransactionSerializerSnapshot
      extends SimpleTypeSerializerSnapshot<Transaction> {

    public TransactionSerializerSnapshot() {
      super(() -> TransactionSerializer.INSTANCE);
    }
  }
}
