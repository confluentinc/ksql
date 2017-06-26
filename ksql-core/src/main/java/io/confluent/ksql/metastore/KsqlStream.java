/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.metastore;

import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.Optional;

public class KsqlStream extends StructuredDataSource {

  public KsqlStream(final String datasourceName, final Schema schema, final Field keyField,
                    final Field timestampField,
                    final KsqlTopic ksqlTopic) {
    super(datasourceName, schema, keyField, timestampField, DataSourceType.KSTREAM, ksqlTopic);
  }

  @Override
  public StructuredDataSource cloneWithTimeKeyColumns() {
    Schema newSchema = SchemaUtil.addImplicitRowTimeRowKeyToSchema(schema);
    return new KsqlStream(dataSourceName, newSchema, keyField, timestampField, ksqlTopic);
  }

  @Override
  public StructuredDataSource cloneWithTimeField(String timestampfieldName) {
    Optional<Field> newTimestampField = SchemaUtil.getFieldByName(schema, timestampfieldName);
    if (newTimestampField.get().schema().type() != Schema.Type.INT64) {
      throw new KsqlException("Timestamp column, " + timestampfieldName + ", should be LONG"
                              + "(INT64).");
    }
    return new KsqlStream(dataSourceName, schema, keyField, newTimestampField.get(), ksqlTopic);
  }


}
