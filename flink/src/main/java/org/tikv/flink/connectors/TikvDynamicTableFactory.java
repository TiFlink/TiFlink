package org.tikv.flink.connectors;

import java.util.Collections;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import shade.com.google.common.collect.ImmutableSet;

public class TikvDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    public static final String IDENTIFIER = "tiflink";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return ImmutableSet.of(
            TikvOptions.PDADDRESS,
            TikvOptions.DATABASE,
            TikvOptions.TABLE
        );
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public DynamicTableSink createDynamicTableSink(final Context context) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(final Context context) {
        final TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();

        final String pdAddress = tableOptions.get(TikvOptions.PDADDRESS);
        final String database = tableOptions.get(TikvOptions.DATABASE);
        final String table = tableOptions.get(TikvOptions.TABLE);

        System.out.println(tableOptions);

        return new TikvDynamicSource(pdAddress, database, table);
    }

}
