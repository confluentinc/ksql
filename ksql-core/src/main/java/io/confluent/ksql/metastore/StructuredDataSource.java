package io.confluent.ksql.metastore;


import io.confluent.ksql.util.KSQLException;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public abstract class StructuredDataSource implements DataSource {

  final String dataSourceName;
  final DataSourceType dataSourceType;
  final Schema schema;
  final Field keyField;

  final KafkaTopic kafkaTopic;


  public StructuredDataSource(String datasourceName, Schema schema, Field keyField,
                              DataSourceType dataSourceType, KafkaTopic kafkaTopic) {
    this.dataSourceName = datasourceName;
    this.schema = schema;
    this.keyField = keyField;
    this.dataSourceType = dataSourceType;
    this.kafkaTopic = kafkaTopic;
  }

  public static DataSourceType getDataSourceType(String dataSourceTypeName) {
    if (dataSourceTypeName.equalsIgnoreCase("stream")) {
      return DataSourceType.KSTREAM;
    } else if (dataSourceTypeName.equalsIgnoreCase("table")) {
      return DataSourceType.KTABLE;
    }
    throw new KSQLException("DataSource Type is not supported: " + dataSourceTypeName);
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

  public KafkaTopic getKafkaTopic() {
    return kafkaTopic;
  }
}
