package io.confluent.ksql.metastore;


import io.confluent.ksql.util.KSQLException;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public abstract class AbstractDataSource implements DataSource {

  final String dataSourceName;
  final DataSourceType dataSourceType;
  final Schema schema;
  final Field keyField;


  public AbstractDataSource(String datasourceName, Schema schema, Field keyField,
                            DataSourceType dataSourceType) {
    this.dataSourceName = datasourceName;
    this.schema = schema;
    this.keyField = keyField;
    this.dataSourceType = dataSourceType;
  }

  public static DataSourceType getDataSpDataSourceType(String dataSourceTypeName) {
    if (dataSourceTypeName.equalsIgnoreCase("stream")) {
      return DataSourceType.KSTREAM;
    } else if (dataSourceTypeName.equalsIgnoreCase("table")) {
      return DataSourceType.KTABLE;
    }
    throw new KSQLException("DataSource Type is not supported: " + dataSourceTypeName);
  }
}
