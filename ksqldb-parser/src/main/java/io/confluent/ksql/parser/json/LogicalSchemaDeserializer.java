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

package io.confluent.ksql.parser.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.parser.SchemaParser;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.io.IOException;

final class LogicalSchemaDeserializer extends JsonDeserializer<LogicalSchema> {
  final boolean withImplicitColumns;

  LogicalSchemaDeserializer(final boolean withImplicitColumns) {
    this.withImplicitColumns = withImplicitColumns;
  }

  @Override
  public LogicalSchema deserialize(
      final JsonParser jp,
      final DeserializationContext ctx
  ) throws IOException {

    final String text = jp.readValueAs(String.class);

    final TableElements tableElements = SchemaParser.parse(text, TypeRegistry.EMPTY);

    return tableElements.toLogicalSchema(withImplicitColumns);
  }
}
