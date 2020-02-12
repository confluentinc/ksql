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

package io.confluent.ksql.api.plugin;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.server.KsqlInsertsException;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlValueCoercer;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import io.vertx.core.json.JsonObject;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

public final class KeyValueExtractor {

  private KeyValueExtractor() {
  }

  public static Struct extractKey(final JsonObject values, final LogicalSchema logicalSchema,
      final SqlValueCoercer sqlValueCoercer) {
    final Struct key = new Struct(logicalSchema.keyConnectSchema());
    for (final Field field : key.schema().fields()) {
      final Object value = values.getValue(field.name());
      if (value == null) {
        throw new KsqlInsertsException("Key field must be specified: " + field.name());
      }
      final Object coercedValue = coerceValue(value,
          SchemaConverters.connectToSqlConverter().toSqlType(field.schema()),
          sqlValueCoercer);
      key.put(field, coercedValue);
    }
    return key;
  }

  public static GenericRow extractValues(final JsonObject values, final LogicalSchema logicalSchema,
      final SqlValueCoercer sqlValueCoercer) {
    final List<Column> valColumns = logicalSchema.value();
    final List<Object> vals = new ArrayList<>(valColumns.size());
    for (Column column : valColumns) {
      final String colName = column.name().name();
      final Object val = values.getValue(colName);
      final Object coercedValue =
          val == null ? null : coerceValue(val, column.type(), sqlValueCoercer);
      vals.add(coercedValue);
    }
    return GenericRow.fromList(vals);
  }

  @SuppressWarnings("unchecked")
  private static Object coerceValue(final Object value, final SqlType sqlType,
      final SqlValueCoercer sqlValueCoercer) {
    if (sqlType instanceof SqlDecimal) {
      // We have to handle this manually as SqlValueCoercer doesn't seem to do it
      final SqlDecimal decType = (SqlDecimal) sqlType;
      if (value instanceof Double) {
        return new BigDecimal(String.valueOf(value))
            .setScale(decType.getScale(), RoundingMode.HALF_UP);
      } else if (value instanceof String) {
        return new BigDecimal((String) value).setScale(decType.getScale(), RoundingMode.HALF_UP);
      } else if (value instanceof Integer) {
        return new BigDecimal((Integer) value).setScale(decType.getScale(), RoundingMode.HALF_UP);
      } else if (value instanceof Long) {
        return new BigDecimal((Long) value).setScale(decType.getScale(), RoundingMode.HALF_UP);
      } else {
        throw createCoerceFailedException(value.getClass(), sqlType);
      }
    }
    return sqlValueCoercer.coerce(value, sqlType)
        .orElseThrow(() -> createCoerceFailedException(value.getClass(), sqlType));
  }

  private static KsqlException createCoerceFailedException(final Class<?> clazz,
      final SqlType sqlType) {
    throw new KsqlException(
        "Can't coerce a value of type " + clazz + " into " + sqlType);
  }
}
