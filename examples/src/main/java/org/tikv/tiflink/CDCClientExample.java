package org.tikv.tiflink;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.tikv.cdc.CDCClient;
import org.tikv.cdc.RegionCDCClient.RegionCDCClientBuilder;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.RangeSplitter;
import org.tikv.common.util.RangeSplitter.RegionTask;
import org.tikv.kvproto.Cdcpb.Event.Row;
import org.tikv.kvproto.Coprocessor.KeyRange;

public class CDCClientExample {
    public static void main(String[] args) throws InterruptedException {
        if (args.length < 3) {
            System.out.println("run with pdAddress, databaseName and tableName");
            System.exit(1);
        }
        final String pdAddress = args[0];
        final String dbName = args[1];
        final String tableName = args[2];
        final TiConfiguration conf = TiConfiguration.createDefault(pdAddress);
        final TiSession session = TiSession.create(conf);

        final TiTableInfo tableInfo = session.getCatalog().getTable(dbName, tableName);
        final KeyRange keyRange = KeyRange.newBuilder()
            .setStart(RowKey.createMin(tableInfo.getId()).toByteString())
            .setEnd(RowKey.createBeyondMax(tableInfo.getId()).toByteString())
            .build();

        final RangeSplitter splitter = RangeSplitter.newSplitter(session.getRegionManager());
        final List<TiRegion> regions = splitter.splitRangeByRegion(Arrays.asList(keyRange))
            .stream().map(RegionTask::getRegion).collect(Collectors.toList());

        final RegionCDCClientBuilder clientBuilder = 
            new RegionCDCClientBuilder(conf, session.getRegionManager(), session.getChannelFactory());
        final TiTimestamp startTs = new TiTimestamp(session.getTimestamp().getPhysical(), 0);
        try(final CDCClient client = new CDCClient(conf, clientBuilder, regions, startTs.getVersion())) {
            client.start();

            while(true) {
                final Row row = client.get();
                if (row == null) {
                    System.out.println("null");
                } else {
                    printRow(tableInfo, row);
                }
            }
        }
    }


    static void printRow(final TiTableInfo tableInfo, final Row row) {
        final RowKey rowKey = RowKey.decode(row.getKey().toByteArray());
        System.out.printf("key: [%d %d]\n", rowKey.getTableId(), rowKey.getHandle());
        System.out.printf("---> Type: %s\n", row.getType());
        System.out.printf("---> OpType: %s\n", row.getOpType());
        System.out.printf("---> StartTs: %d\n", row.getStartTs());
        System.out.printf("---> CommitTs: %d\n", row.getCommitTs());
        System.out.printf("---> key: %s\n", row.getKey());
        System.out.printf("---> value: %s\n", row.getValue());
        System.out.flush();
    }
}
