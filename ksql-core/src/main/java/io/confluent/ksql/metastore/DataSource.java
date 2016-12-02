package io.confluent.ksql.metastore;


import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public interface DataSource {

  public static enum DataSourceType {KSTREAM, KTABLE}

  ;

  public String getName();

  public Schema getSchema();

  public Field getKeyField();

  public DataSourceType getDataSourceType();
}
