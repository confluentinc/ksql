/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.kql.metastore;

public interface DataSource {

  public static enum DataSourceType { KTOPIC, KSTREAM, KTABLE }

  public static enum DataSourceSerDe { JSON, AVRO, CSV }

  public static final String AVRO_SERDE_NAME = "avro";
  public static final String JSON_SERDE_NAME = "json";
  public static final String CSV_SERDE_NAME = "csv";

  public String getName();

  public DataSourceType getDataSourceType();

}
