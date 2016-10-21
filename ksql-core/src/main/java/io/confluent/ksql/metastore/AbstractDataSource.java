package io.confluent.ksql.metastore;


import org.apache.kafka.connect.data.Schema;

public abstract class AbstractDataSource implements DataSource {

    final String dataSourceName;
    final DataSourceType dataSourceType;
    final Schema schema;

    public AbstractDataSource(String datasourceName, Schema schema, DataSourceType dataSourceType) {
        this.dataSourceName = datasourceName;
        this.schema = schema;
        this.dataSourceType = dataSourceType;
    }
}
