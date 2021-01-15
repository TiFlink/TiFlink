package org.tikv.cdc;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.tikv.common.region.TiRegion;
import shade.com.google.common.base.Preconditions;

public class RegionStateManager {
    private final long[] minSegTree;
    private final long[] maxSegTree;
    private final Map<Long, Integer> indexMap;
    private final Set<Long> initializedSet;

    public RegionStateManager(final List<TiRegion> regions) {
        this(regions.stream().mapToLong(TiRegion::getId).toArray());
    }

    public RegionStateManager(final long[] regionIds) {
        Preconditions.checkArgument(regionIds.length > 0, "regionIds can't be empty");
        int offset = 1;
        while (offset < regionIds.length) offset *= 2;

        minSegTree = new long[offset + regionIds.length];
        maxSegTree = new long[offset + regionIds.length];

        indexMap = new HashMap<>(regionIds.length);
        for (int i = 0; i < regionIds.length; i++) {
            indexMap.put(regionIds[i], offset + i);
        }
        initializedSet = new HashSet<>();
    }

    public boolean markInitialized(final long regionId) {
        initializedSet.add(regionId);
        return isInitialized();
    }

    public boolean isInitialized() {
        return initializedSet.size() == indexMap.size();
    }

    public long updateTs(final long regionId, final long resolvedTs) {
        int index = indexMap.get(regionId);
        minSegTree[index] = resolvedTs;

        while (index > 0) {
            index /= 2;
            minSegTree[index] = Math.min(
                index * 2, 
                index * 2 + 1 < minSegTree.length ? minSegTree[index * 2 + 1] : Long.MAX_VALUE
            );

            maxSegTree[index] = Math.max(
                index * 2, 
                index * 2 + 1 < minSegTree.length ? minSegTree[index * 2 + 1] : Long.MIN_VALUE
            );
        };

        return minSegTree[0];
    }

    public long getMinTs() {
        return minSegTree[0];
    }

    public long getMaxTs() {
        return maxSegTree[0];
    }
}
