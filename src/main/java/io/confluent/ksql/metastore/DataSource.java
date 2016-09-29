package io.confluent.ksql.metastore;


import io.confluent.ksql.planner.Schema;

public interface DataSource {

    public static enum DataSourceType {STREAM, TABLE};

    public String getName();
    public Schema getSchema();
    public DataSourceType getDataSourceType();
}
