/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.util;

import io.confluent.ksql.rest.entity.FieldInfo;
import io.confluent.ksql.rest.entity.SchemaInfo;
import org.apache.kafka.connect.data.Schema;

import java.util.List;
import java.util.stream.Collectors;

public class EntityUtil {
  public static List<FieldInfo> buildSourceSchemaEntity(final Schema schema) {
    return buildSchemaEntity(schema).getFields()
        .orElseThrow(() -> new RuntimeException("Root schema should contain fields"));
  }

  private static SchemaInfo buildSchemaEntity(final Schema schema) {
    switch (schema.type()) {
      case ARRAY:
      case MAP:
        return new SchemaInfo(
            getSchemaTypeString(schema.type()),
            null,
            buildSchemaEntity(schema.valueSchema())
        );
      case STRUCT:
        return new SchemaInfo(
            getSchemaTypeString(schema.type()),
            schema.fields()
                .stream()
                .map(
                    f -> new FieldInfo(f.name(), buildSchemaEntity(f.schema())))
                .collect(Collectors.toList()),
            null
        );
      default:
        return new SchemaInfo(getSchemaTypeString(schema.type()), null, null);
    }
  }

  private static SchemaInfo.Type getSchemaTypeString(final Schema.Type type) {
    switch (type) {
      case INT32:
        return SchemaInfo.Type.INTEGER;
      case INT64:
        return SchemaInfo.Type.BIGINT;
      case FLOAT32:
      case FLOAT64:
        return SchemaInfo.Type.DOUBLE;
      case BOOLEAN:
        return SchemaInfo.Type.BOOLEAN;
      case STRING:
        return SchemaInfo.Type.STRING;
      case ARRAY:
        return SchemaInfo.Type.ARRAY;
      case MAP:
        return SchemaInfo.Type.MAP;
      case STRUCT:
        return SchemaInfo.Type.STRUCT;
      default:
        throw new RuntimeException(String.format("Invalid type in schema: %s.", type.getName()));
    }
  }
}
