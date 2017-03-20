/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.kql.metastore;

import io.confluent.kql.util.KQLException;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public abstract class StructuredDataSource implements DataSource {

  final String dataSourceName;
  final DataSourceType dataSourceType;
  final Schema schema;
  final Field keyField;

  final KQLTopic kqlTopic;


  public StructuredDataSource(final String datasourceName, final Schema schema,
                              final Field keyField,
                              final DataSourceType dataSourceType, final KQLTopic kqlTopic) {
    this.dataSourceName = datasourceName;
    this.schema = schema;
    this.keyField = keyField;
    this.dataSourceType = dataSourceType;
    this.kqlTopic = kqlTopic;
  }

  public static DataSourceType getDataSourceType(String dataSourceTypeName) {
    if ("STREAM".equals(dataSourceTypeName)) {
      return DataSourceType.KSTREAM;
    } else if ("TABLE".equals(dataSourceTypeName)) {
      return DataSourceType.KTABLE;
    }
    throw new KQLException("DataSource Type is not supported: " + dataSourceTypeName);
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

  public KQLTopic getKqlTopic() {
    return kqlTopic;
  }
}
