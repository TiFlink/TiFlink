package org.tikv.flink.connectors;

import java.util.Objects;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.meta.TiTableInfo;

public class TikvDynamicSink implements DynamicTableSink {

    private final String pdAddress;
    private final String database;
    private final String table;
    private final DataType physicalDataType;

    public TikvDynamicSink(final String pdAddress, final String database, final String table, final DataType dataType) {
        this.pdAddress = pdAddress;
        this.database = database;
        this.table = table;
        this.physicalDataType = dataType;
    }


    @Override
    public ChangelogMode getChangelogMode(final ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
            .addContainedKind(RowKind.INSERT)
            .addContainedKind(RowKind.DELETE)
            .addContainedKind(RowKind.UPDATE_AFTER)
            .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(final Context context) {
        final TiConfiguration conf = TiConfiguration.createDefault(pdAddress);
        try (final TiSession session = TiSession.create(conf)) {
            final TiTableInfo tableInfo = session.getCatalog().getTable(database, table);
            Objects.nonNull(tableInfo);

            return SinkFunctionProvider.of(new FlinkTikvProducer(conf, tableInfo, physicalDataType));
        } catch(final Throwable e){
            throw new RuntimeException("Can't create consumer", e);
        }
    }

    @Override
    public DynamicTableSink copy() {
        return new TikvDynamicSink(pdAddress, database, table, physicalDataType);
    }

    @Override
    public String asSummaryString() {
        return String.format("TiKV Table[`%s`.`%s`]", database, table);
    }

}
