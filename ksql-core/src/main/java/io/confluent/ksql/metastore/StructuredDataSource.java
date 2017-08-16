/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.metastore;

import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public abstract class StructuredDataSource implements DataSource {

  final String dataSourceName;
  final DataSourceType dataSourceType;
  final Schema schema;
  final Field keyField;
  final Field timestampField;

  final KsqlTopic ksqlTopic;


  public StructuredDataSource(final String datasourceName, final Schema schema,
                              final Field keyField,
                              final Field timestampField,
                              final DataSourceType dataSourceType, final KsqlTopic ksqlTopic) {
    this.dataSourceName = datasourceName;
    this.schema = schema;
    this.keyField = keyField;
    this.timestampField = timestampField;
    this.dataSourceType = dataSourceType;
    this.ksqlTopic = ksqlTopic;
  }

  public static DataSourceType getDataSourceType(String dataSourceTypeName) {
    switch (dataSourceTypeName) {
      case "STREAM":
        return DataSourceType.KSTREAM;
      case "TABLE":
        return DataSourceType.KTABLE;
      default:
        throw new KsqlException("DataSource Type is not supported: " + dataSourceTypeName);
    }
  }

  @Override
  public String getName() {
    return this.dataSourceName;
  }

  public Schema getSchema() {
    return this.schema;
  }

  public Field getKeyField() {
    return this.keyField;
  }

  @Override
  public DataSourceType getDataSourceType() {
    return this.dataSourceType;
  }

  public KsqlTopic getKsqlTopic() {
    return ksqlTopic;
  }

  public Field getTimestampField() {
    return timestampField;
  }

  public abstract StructuredDataSource cloneWithTimeKeyColumns();

  public abstract StructuredDataSource cloneWithTimeField(String timestampfieldName);
}
