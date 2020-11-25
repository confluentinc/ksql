/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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
import io.confluent.ksql.schema.ksql.Column;
import java.io.IOException;

public class ColumnDeserializor extends JsonDeserializer<Column> {

  public ColumnDeserializor() {
  }

  @Override
  public Column deserialize(
      final JsonParser p,
      final DeserializationContext ctxt
  ) throws IOException {
    final String text = p.readValueAs(String.class);
    return SchemaParser.parse(text, TypeRegistry.EMPTY).toLogicalSchema().columns().get(0);
  }
}
