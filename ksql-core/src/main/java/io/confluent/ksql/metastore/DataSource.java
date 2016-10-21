package io.confluent.ksql.metastore;


import org.apache.kafka.connect.data.Schema;

public interface DataSource {

    public static enum DataSourceType {STREAM, TABLE};

    public String getName();
    public Schema getSchema();
    public DataSourceType getDataSourceType();
}
