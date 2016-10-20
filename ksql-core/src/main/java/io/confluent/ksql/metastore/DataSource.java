package io.confluent.ksql.metastore;


import io.confluent.ksql.planner.KSQLSchema;

public interface DataSource {

    public static enum DataSourceType {STREAM, TABLE};

    public String getName();
    public KSQLSchema getKSQLSchema();
    public DataSourceType getDataSourceType();
}
