package io.confluent.ksql.metastore;

import io.confluent.ksql.planner.KSQLSchema;

public class KafkaTopic extends AbstractDataSource {

    final String topicName;

    public KafkaTopic(String datasourceName, KSQLSchema ksqlSchema, DataSourceType dataSourceType, String topicName) {
        super(datasourceName, ksqlSchema, dataSourceType);
        this.topicName = topicName;
    }

    @Override
    public String getName() {
        return this.dataSourceName;
    }

    @Override
    public KSQLSchema getKSQLSchema() {
        return this.ksqlSchema;
    }

    @Override
    public DataSourceType getDataSourceType() {
        return this.dataSourceType;
    }

    public String getTopicName() {
        return topicName;
    }
}
