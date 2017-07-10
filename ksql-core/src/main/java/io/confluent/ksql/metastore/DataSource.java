/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.metastore;

public interface DataSource {

  public static enum DataSourceType { KTOPIC, KSTREAM, KTABLE }

  public static enum DataSourceSerDe { JSON, AVRO, DELIMITED }

  public static final String AVRO_SERDE_NAME = "AVRO";
  public static final String JSON_SERDE_NAME = "JSON";
  public static final String DELIMITED_SERDE_NAME = "DELIMITED";

  public String getName();

  public DataSourceType getDataSourceType();

}
