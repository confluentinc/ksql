/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.metastore;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import io.confluent.ksql.util.KSQLException;
import io.confluent.ksql.util.SchemaUtil;

public class KSQLStream extends StructuredDataSource {

  public KSQLStream(final String datasourceName, final Schema schema, final Field keyField,
                    final Field timestampField,
                    final KSQLTopic ksqlTopic) {
    super(datasourceName, schema, keyField, timestampField, DataSourceType.KSTREAM, ksqlTopic);
  }

  @Override
  public StructuredDataSource cloneWithTimeKeyColumns() {
    Schema newSchema = SchemaUtil.addImplicitRowTimeRowKeyToSchema(schema);
    return new KSQLStream(dataSourceName, newSchema, keyField, timestampField, ksqlTopic);
  }

  @Override
  public StructuredDataSource cloneWithTimeField(String timestampfieldName) {
    Field newTimestampField = SchemaUtil.getFieldByName(schema, timestampfieldName);
    if (newTimestampField.schema().type() != Schema.Type.INT64) {
      throw new KSQLException("Timestamp column, " + timestampfieldName + ", should be LONG"
                              + "(INT64).");
    }
    return new KSQLStream(dataSourceName, schema, keyField, newTimestampField, ksqlTopic);
  }


}
