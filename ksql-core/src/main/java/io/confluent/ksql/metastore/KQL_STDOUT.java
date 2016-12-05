package io.confluent.ksql.metastore;


import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class KQL_STDOUT extends AbstractDataSource {

  public static final String KQL_STDOUT_NAME = "KQL_STDOUT_NAME";

  public KQL_STDOUT(String datasourceName, Schema schema, Field keyField,
                    DataSourceType dataSourceType) {
    super(datasourceName,schema, keyField, dataSourceType);
  }


  @Override
  public String getName() {
    return null;
  }

  @Override
  public Schema getSchema() {
    return null;
  }

  @Override
  public Field getKeyField() {
    return null;
  }

  @Override
  public DataSourceType getDataSourceType() {
    return null;
  }
}
