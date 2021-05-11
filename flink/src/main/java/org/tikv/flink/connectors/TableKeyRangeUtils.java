package org.tikv.flink.connectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.math.BigInteger;
import java.util.List;
import org.tikv.common.key.RowKey;
import org.tikv.common.util.KeyRangeUtils;
import org.tikv.kvproto.Coprocessor.KeyRange;

public class TableKeyRangeUtils {
  public static KeyRange getTableKeyRange(final long tableId) {
    return KeyRangeUtils.makeCoprocRange(
        RowKey.createMin(tableId).toByteString(), RowKey.createBeyondMax(tableId).toByteString());
  }

  public static List<KeyRange> getTableKeyRanges(final long tableId, final int num) {
    Preconditions.checkArgument(num > 0, "Illegal value of num");

    if (num == 1) {
      return ImmutableList.of(getTableKeyRange(tableId));
    }

    final long delta =
        BigInteger.valueOf(Long.MAX_VALUE)
            .subtract(BigInteger.valueOf(Long.MIN_VALUE + 1))
            .divide(BigInteger.valueOf(num))
            .longValueExact();
    final ImmutableList.Builder<KeyRange> builder = ImmutableList.builder();
    for (int i = 0; i < num; i++) {
      final RowKey startKey =
          (i == 0)
              ? RowKey.createMin(tableId)
              : RowKey.toRowKey(tableId, Long.MIN_VALUE + delta * i);
      final RowKey endKey =
          (i == num - 1)
              ? RowKey.createBeyondMax(tableId)
              : RowKey.toRowKey(tableId, Long.MIN_VALUE + delta * (i + 1));
      builder.add(KeyRangeUtils.makeCoprocRange(startKey.toByteString(), endKey.toByteString()));
    }
    return builder.build();
  }

  public static KeyRange getTableKeyRange(final long tableId, final int num, final int idx) {
    Preconditions.checkArgument(idx >= 0 && idx < num, "Illegal value of idx");
    return getTableKeyRanges(tableId, num).get(idx);
  }
}
