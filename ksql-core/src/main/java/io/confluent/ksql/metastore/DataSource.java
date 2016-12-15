package io.confluent.ksql.metastore;


import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public interface DataSource {

  public static enum DataSourceType {KTOPIC, KSTREAM, KTABLE}

  public static enum DataSourceSerDe {JSON, AVRO}


  public String getName();

  public DataSourceType getDataSourceType();

}
