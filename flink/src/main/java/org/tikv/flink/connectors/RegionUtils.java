package org.tikv.flink.connectors;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.tikv.common.TiSession;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.RangeSplitter;
import org.tikv.common.util.RangeSplitter.RegionTask;
import org.tikv.kvproto.Coprocessor.KeyRange;

public class RegionUtils {
    public static List<TiRegion> getTableRegions(final TiSession session, final TiTableInfo tableInfo) {
        return getTableRegions(session, tableInfo.getId());
    }

    public static List<TiRegion> getTableRegions(final TiSession session, final long tableId) {
        final KeyRange keyRange = KeyRange.newBuilder()
            .setStart(RowKey.createMin(tableId).toByteString())
            .setEnd(RowKey.createBeyondMax(tableId).toByteString())
            .build();

        final RangeSplitter splitter = RangeSplitter.newSplitter(session.getRegionManager());
        return splitter.splitRangeByRegion(Arrays.asList(keyRange))
            .stream().map(RegionTask::getRegion).collect(Collectors.toList());
    }
}
