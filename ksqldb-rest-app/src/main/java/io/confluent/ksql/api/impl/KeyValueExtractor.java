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

package io.confluent.ksql.api.impl;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.server.KsqlApiException;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SqlValueCoercer;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.JsonUtil;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;

public final class KeyValueExtractor {

  private KeyValueExtractor() {
  }

  public static GenericKey extractKey(
      final JsonObject values,
      final LogicalSchema logicalSchema,
      final SqlValueCoercer sqlValueCoercer
  ) {
    final List<Column> keyColumns = logicalSchema.key();
    final GenericKey.Builder builder = GenericKey.builder(logicalSchema);
    for (final Column keyColumn : keyColumns) {
      final Object value = values.getValue(keyColumn.name().text());
      if (value == null) {
        throw new KsqlApiException("Key field must be specified: " + keyColumn.name().text(),
            Errors.ERROR_CODE_BAD_REQUEST);
      }
      final Object coercedValue = coerceObject(value, keyColumn.type(), sqlValueCoercer);
      builder.append(coercedValue);
    }
    return builder.build();
  }

  public static GenericRow extractValues(
      final JsonObject values,
      final LogicalSchema logicalSchema,
      final SqlValueCoercer sqlValueCoercer
  ) {
    final List<Column> valColumns = logicalSchema.value();
    final List<Object> vals = new ArrayList<>(valColumns.size());
    for (Column column : valColumns) {
      final String colName = column.name().text();
      final Object val = values.getValue(colName);
      final Object coercedValue =
          val == null ? null : coerceObject(val, column.type(), sqlValueCoercer);
      vals.add(coercedValue);
    }
    return GenericRow.fromList(vals);
  }

  static JsonObject convertColumnNameCase(final JsonObject jsonObjectWithCaseInsensitiveFields) {
    try {
      return JsonUtil.convertJsonFieldCase(jsonObjectWithCaseInsensitiveFields);
    } catch (IllegalArgumentException e) {
      throw new KsqlApiException(e.getMessage(), Errors.ERROR_CODE_BAD_REQUEST);
    }
  }

  private static Object coerceObject(
      final Object value,
      final SqlType sqlType,
      final SqlValueCoercer sqlValueCoercer
  ) {
    return sqlValueCoercer.coerce(value, sqlType)
        .orElseThrow(() -> new KsqlApiException(
            String.format("Can't coerce a field of type %s (%s) into type %s", value.getClass(),
                value, sqlType),
            Errors.ERROR_CODE_BAD_REQUEST))
        .orElse(null);
  }

}
