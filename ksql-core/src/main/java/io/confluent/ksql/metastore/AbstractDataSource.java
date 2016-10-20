package io.confluent.ksql.metastore;


import io.confluent.ksql.planner.KSQLSchema;

public abstract class AbstractDataSource implements DataSource {

    final String dataSourceName;
    final DataSourceType dataSourceType;
    final KSQLSchema ksqlSchema;

    public AbstractDataSource(String datasourceName, KSQLSchema ksqlSchema, DataSourceType dataSourceType) {
        this.dataSourceName = datasourceName;
        this.ksqlSchema = ksqlSchema;
        this.dataSourceType = dataSourceType;
    }
}
