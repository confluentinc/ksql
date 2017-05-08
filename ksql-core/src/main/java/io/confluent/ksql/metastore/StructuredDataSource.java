/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.metastore;

import io.confluent.ksql.util.KSQLException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public abstract class StructuredDataSource implements DataSource {

  final String dataSourceName;
  final DataSourceType dataSourceType;
  final Schema schema;
  final Field keyField;

  final KSQLTopic ksqlTopic;


  public StructuredDataSource(final String datasourceName, final Schema schema,
                              final Field keyField,
                              final DataSourceType dataSourceType, final KSQLTopic ksqlTopic) {
    this.dataSourceName = datasourceName;
    this.schema = schema;
    this.keyField = keyField;
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
        throw new KSQLException("DataSource Type is not supported: " + dataSourceTypeName);
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

  public KSQLTopic getKsqlTopic() {
    return ksqlTopic;
  }
}
