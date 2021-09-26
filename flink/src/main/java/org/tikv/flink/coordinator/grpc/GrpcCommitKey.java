package org.tikv.flink.coordinator.grpc;

import org.tikv.common.codec.Codec.IntegerCodec;
import org.tikv.common.codec.CodecDataOutput;
import org.tikv.common.key.Key;

class GrpcCommitKey extends Key {
  private static final byte[] REC_PREFIX_SEP = new byte[] {'_', 'f'};

  GrpcCommitKey(final long tableId, final long checkpointId, final long startTs) {
    super(encode(tableId, checkpointId, startTs));
  }

  static byte[] encode(final long tableId, final long checkpointId, final long startTs) {
    final CodecDataOutput cdo = new CodecDataOutput();
    cdo.write(TBL_PREFIX);
    IntegerCodec.writeLong(cdo, tableId);
    cdo.write(REC_PREFIX_SEP);
    IntegerCodec.writeLong(cdo, checkpointId);
    IntegerCodec.writeLong(cdo, startTs);
    return cdo.toBytes();
  }
}
