/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.util;

import avro.shaded.com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.entity.FieldInfo;
import io.confluent.ksql.rest.entity.SchemaInfo;
import io.confluent.ksql.util.DecimalUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;

public final class EntityUtil {
  private static final Map<Schema.Type, Function<Schema, SchemaInfo.Type>>
      SCHEMA_TYPE_TO_SCHEMA_INFO_TYPE =
      ImmutableMap.<Schema.Type, Function<Schema, SchemaInfo.Type>>builder()
          .put(Schema.Type.INT32, s -> SchemaInfo.Type.INTEGER)
          .put(Schema.Type.INT64, s -> SchemaInfo.Type.BIGINT)
          .put(Schema.Type.FLOAT32, s -> SchemaInfo.Type.DOUBLE)
          .put(Schema.Type.FLOAT64, s -> SchemaInfo.Type.DOUBLE)
          .put(Schema.Type.BOOLEAN, s -> SchemaInfo.Type.BOOLEAN)
          .put(Schema.Type.STRING, s -> SchemaInfo.Type.STRING)
          .put(Schema.Type.BYTES, EntityUtil::getLogicalType)
          .put(Schema.Type.ARRAY, s -> SchemaInfo.Type.ARRAY)
          .put(Schema.Type.MAP, s -> SchemaInfo.Type.MAP)
          .put(Schema.Type.STRUCT, s -> SchemaInfo.Type.STRUCT)
          .build();

  private EntityUtil() {
  }

  public static List<FieldInfo> buildSourceSchemaEntity(final Schema schema) {
    return buildSchemaEntity(schema).getFields()
        .orElseThrow(() -> new RuntimeException("Root schema should contain fields"));
  }

  private static SchemaInfo buildSchemaEntity(final Schema schema) {
    switch (schema.type()) {
      case ARRAY:
      case MAP:
        return new SchemaInfo(
            getSchemaTypeString(schema),
            null,
            buildSchemaEntity(schema.valueSchema()),
            null
        );
      case STRUCT:
        return new SchemaInfo(
            getSchemaTypeString(schema),
            schema.fields()
                .stream()
                .map(
                    f -> new FieldInfo(f.name(), buildSchemaEntity(f.schema())))
                .collect(Collectors.toList()),
            null,
            null
        );
      case BYTES:
        if (DecimalUtil.isDecimalSchema(schema)) {
          final String precision = Integer.toString(DecimalUtil.getPrecision(schema));
          final String scale = Integer.toString(DecimalUtil.getScale(schema));

          return new SchemaInfo(
              getSchemaTypeString(schema),
              null,
              null,
              Arrays.asList(precision, scale)
          );
        }

        // A BYTES type with a logical type other than Decimal is not expected, so let's
        // return a SchemaInfo like the below default case
        return new SchemaInfo(getSchemaTypeString(schema), null, null, null);
      default:
        return new SchemaInfo(getSchemaTypeString(schema), null, null, null);
    }
  }

  private static SchemaInfo.Type getSchemaTypeString(final Schema schema) {
    final Function<Schema, SchemaInfo.Type> handler =
        SCHEMA_TYPE_TO_SCHEMA_INFO_TYPE.get(schema.type());
    if (handler == null) {
      throw new RuntimeException(String.format("Invalid type in schema: %s.",
          schema.type().getName()));
    }

    return handler.apply(schema);
  }

  private static SchemaInfo.Type getLogicalType(final Schema schema) {
    if (DecimalUtil.isDecimalSchema(schema)) {
      return SchemaInfo.Type.DECIMAL;
    }

    throw new RuntimeException(String.format("Invalid type in schema: %s.",
        schema.type().getName()));
  }
}
