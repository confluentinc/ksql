package io.confluent.ksql.metastore;

import org.apache.kafka.connect.data.Schema;

public class KafkaTopic extends AbstractDataSource {

    final String topicName;

    public KafkaTopic(String datasourceName, Schema schema, DataSourceType dataSourceType, String topicName) {
        super(datasourceName, schema, dataSourceType);
        this.topicName = topicName;
    }

    @Override
    public String getName() {
        return this.dataSourceName;
    }

    @Override
    public Schema getSchema() {
        return this.schema;
    }

    @Override
    public DataSourceType getDataSourceType() {
        return this.dataSourceType;
    }

    public String getTopicName() {
        return topicName;
    }
}
