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

package io.confluent.ksql.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.io.IOException;
import java.util.Objects;

/**
 * Custom Jackson JSON serializer for {@link LogicalSchema}.
 *
 * <p>The schema is serialized as a simple SQL string
 */
public final class LogicalSchemaSerializer extends JsonSerializer<LogicalSchema> {
  private final FormatOptions formatOptions;

  public LogicalSchemaSerializer(final FormatOptions formatOptions) {
    this.formatOptions = Objects.requireNonNull(formatOptions, "formatOptions");
  }

  @Override
  public void serialize(
      final LogicalSchema schema,
      final JsonGenerator gen,
      final SerializerProvider serializerProvider
  ) throws IOException {
    final String text = schema.toString(formatOptions);
    gen.writeString(trimArrayBrackets(text));
  }

  private static String trimArrayBrackets(final String text) {
    return text.substring(1, text.length() - 1);
  }
}
