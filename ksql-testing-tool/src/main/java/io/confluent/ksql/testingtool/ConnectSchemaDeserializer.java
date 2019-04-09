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

package io.confluent.ksql.testingtool;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.schema.ksql.LogicalSchemas;
import io.confluent.ksql.schema.ksql.LogicalSchemas.SqlTypeToLogicalConverter;
import java.io.IOException;
import java.util.Optional;
import org.apache.kafka.connect.data.ConnectSchema;

public class ConnectSchemaDeserializer extends StdDeserializer<Optional<ConnectSchema>> {

  private final SqlTypeToLogicalConverter sqlTypeToLogicalConverter =
      LogicalSchemas.fromSqlTypeConverter();

  public ConnectSchemaDeserializer() {
    super(ConnectSchema.class);
  }

  @Override
  public Optional<ConnectSchema> deserialize(
      final JsonParser jp,
      final DeserializationContext ctxt
  ) throws IOException {

    final JsonNode node = jp.getCodec().readTree(jp);

    final String type = node.get("type").traverse(jp.getCodec()).readValueAs(String.class);
    if (type == null) {
      throw new MissingFieldException("type");
    }

    try {
      final PrimitiveType sqlType = PrimitiveType.of(type.toUpperCase());

      return Optional.of((ConnectSchema) sqlTypeToLogicalConverter.fromSqlType(sqlType));
    } catch (final Exception e) {
      throw new InvalidFieldException("type", "only primitive types supported", e);
    }
  }
}