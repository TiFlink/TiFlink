/*
 * Copyright 2020 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tikv.common.codec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.types.IntegerType;

public class TiTableCodec {

    /**
     * New Row Format: Reference
     * https://github.com/pingcap/tidb/blob/952d1d7541a8e86be0af58f5b7e3d5e982bab34e/docs/design/2018-07-19-row-format.md
     *
     * <p>- version, flag, numOfNotNullCols, numOfNullCols, notNullCols, nullCols, notNullOffsets,
     * notNullValues
     */
    public static byte[] encodeRow(
            List<TiColumnInfo> columnInfos, Object[] values, boolean isPkHandle) {
        RowEncoderV2 encoder = new RowEncoderV2();
        List<TiColumnInfo> columnInfoList = new ArrayList<>();
        List<Object> valueList = new ArrayList<>();
        for (int i = 0; i < columnInfos.size(); i++) {
            TiColumnInfo col = columnInfos.get(i);
            // skip pk is handle case
            if (col.isPrimaryKey() && isPkHandle) {
                continue;
            }
            columnInfoList.add(col);
            valueList.add(values[i]);
        }
        return encoder.encode(columnInfoList, valueList);
    }

    public static Object[] decodeKeyOnly(final Long handle, final TiTableInfo tableInfo) {
        if (handle == null && tableInfo.isPkHandle()) {
            throw new IllegalArgumentException("when pk is handle, handle cannot be null");
        }

        final int colSize = tableInfo.getColumns().size();
        Object[] res = new Object[colSize];

        for (int i = 0; i < colSize; i++) {
            final TiColumnInfo col = tableInfo.getColumn(i);
            if (col.isPrimaryKey() && tableInfo.isPkHandle()) {
                res[i] = handle;
                return res;
            }
        }

        throw new IllegalArgumentException("PK not found!");
    }
    public static Object[] decodeRow(byte[] value, Long handle, TiTableInfo tableInfo) {
        if ((value[0] & 0xff) == org.tikv.common.codec.RowV2.CODEC_VER) {
            return decodeRowV2(value, handle, tableInfo);
        }
        return decodeRowV1(value, handle, tableInfo);
    }

    public static Object[] decodeRowV1(byte[] value, Long handle, TiTableInfo tableInfo) {
        if (handle == null && tableInfo.isPkHandle()) {
            throw new IllegalArgumentException("when pk is handle, handle cannot be null");
        }

        int colSize = tableInfo.getColumns().size();
        HashMap<Long, TiColumnInfo> idToColumn = new HashMap<>(colSize);
        for (TiColumnInfo col : tableInfo.getColumns()) {
            idToColumn.put(col.getId(), col);
        }

        // decode bytes to Map<ColumnID, Data>
        HashMap<Long, Object> decodedDataMap = new HashMap<>(colSize);
        CodecDataInput cdi = new CodecDataInput(value);
        Object[] res = new Object[colSize];
        while (!cdi.eof()) {
            try {
                long colID = (long)IntegerType.BIGINT.decode(cdi);
                Object colValue = idToColumn.get(colID).getType().decodeForBatchWrite(cdi);
                decodedDataMap.put(colID, colValue);
            } catch (final Throwable e) {
                continue;
            }
        }

        // construct Row with Map<ColumnID, Data> & handle
        for (int i = 0; i < colSize; i++) {
            // skip pk is handle case
            TiColumnInfo col = tableInfo.getColumn(i);
            if (col.isPrimaryKey() && tableInfo.isPkHandle()) {
                res[i] = handle;
            } else {
                res[i] = decodedDataMap.get(col.getId());
            }
        } 
        return res;
    }

    public static Object[] decodeRowV2(byte[] value, Long handle, TiTableInfo tableInfo) {
        if (handle == null && tableInfo.isPkHandle()) {
            throw new IllegalArgumentException("when pk is handle, handle cannot be null");
        }

        int colSize = tableInfo.getColumns().size();
        Object[] res = new Object[colSize];

        org.tikv.common.codec.RowV2 rowV2 = org.tikv.common.codec.RowV2.createNew(value);
        for (int i = 0; i < colSize; i++) {
            // decode bytes to Map<ColumnID, Data>
            final TiColumnInfo col = tableInfo.getColumn(i);

            if (col.isPrimaryKey() && tableInfo.isPkHandle()) {
                res[i] = handle;
                continue;
            }
            org.tikv.common.codec.RowV2.ColIDSearchResult searchResult = rowV2.findColID(col.getId());
            if (searchResult.isNull) {
                // current col is null, nothing should be added to decodedMap
                continue;
            }
            if (!searchResult.notFound) {
                // corresponding column should be found
                assert (searchResult.idx != -1);
                byte[] colData = rowV2.getData(searchResult.idx);
                res[i] = RowDecoderV2.decodeCol(colData, col.getType());
            }
        }
        return res;
    }
}

