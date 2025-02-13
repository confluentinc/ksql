/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.serde.connect;

import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SchemaConverters.SqlToConnectTypeConverter;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.util.DecimalUtil;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public final class ConnectSchemas {

  private ConnectSchemas() {
  }

  /**
   * Convert a list of columns into a Connect Struct schema with fields to match.
   *
   * @param columns the list of columns.
   * @return the Struct schema.
   */
  public static ConnectSchema columnsToConnectSchema(final List<? extends SimpleColumn> columns) {
    final SqlToConnectTypeConverter converter = SchemaConverters.sqlToConnectConverter();

    final SchemaBuilder builder = SchemaBuilder.struct();
    for (final SimpleColumn column : columns) {
      final Schema colSchema = converter.toConnectSchema(column.type());
      builder.field(column.name().text(), colSchema);
    }

    return (ConnectSchema) builder.build();
  }

  public static Object withCompatibleSchema(final Schema schema, final Object object) {
    if (object == null) {
      return null;
    }
    switch (schema.type()) {
      case ARRAY:
        final List<Object> ksqlArray = new ArrayList<>(((List) object).size());
        ((List) object).forEach(
            e -> ksqlArray.add(withCompatibleSchema(schema.valueSchema(), e)));
        return ksqlArray;

      case MAP:
        final Map<Object, Object> ksqlMap = new HashMap<>();
        ((Map<Object, Object>) object).forEach(
            (key, value) -> ksqlMap.put(
                withCompatibleSchema(schema.keySchema(), key),
                withCompatibleSchema(schema.valueSchema(), value)
            ));
        return ksqlMap;

      case STRUCT:
        return withCompatibleRowSchema((Struct) object, schema);
      case BYTES:
        if (DecimalUtil.isDecimal(schema)) {
          return DecimalUtil.ensureFit((BigDecimal) object, schema);
        } else {
          return object;
        }
      default:
        return object;
    }
  }

  public static Struct withCompatibleRowSchema(final Struct source, final Schema targetSchema) {
    final Struct struct = new Struct(targetSchema);
    final Schema originalSchema = source.schema();

    final Iterator<Field> sourceIt = originalSchema.fields().iterator();

    for (final Field targetField : targetSchema.fields()) {
      final Field sourceField = sourceIt.next();
      final Object value = source.get(sourceField);
      final Object adjusted = withCompatibleSchema(targetField.schema(), value);
      struct.put(targetField, adjusted);
    }

    return struct;
  }
}
