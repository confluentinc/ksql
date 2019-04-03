/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.schema.connect;

import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;

public class DefaultSchemaFormatter implements SchemaFormatter {

  @Override
  public String format(final Schema schema) {
    return formatSchema(schema);
  }

  private static String formatSchema(final Schema schema) {
    final String type = formatSchemaType(schema);
    if (schema.isOptional()) {
      return "optional<" + type + ">";
    }
    return type;
  }

  private static String formatSchemaType(final Schema schema) {
    switch (schema.type()) {
      case ARRAY:
        return schema.type().getName() + "<"
            + formatSchema(schema.valueSchema())
            + ">";

      case MAP:
        return schema.type().getName() + "<"
            + formatSchema(schema.keySchema()) + ", "
            + formatSchema(schema.valueSchema())
            + ">";

      case STRUCT:
        return schema.type().getName() + "<"
            + schema.fields().stream()
            .map(field -> field.name() + " " + formatSchema(field.schema()))
            .collect(Collectors.joining(","))
            + ">";

      default:
        return schema.type().getName();
    }
  }
}
