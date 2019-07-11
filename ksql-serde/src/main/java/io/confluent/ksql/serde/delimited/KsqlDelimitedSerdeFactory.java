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

package io.confluent.ksql.serde.delimited;

import com.google.errorprone.annotations.Immutable;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.connect.SchemaWalker;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;


@Immutable
public class KsqlDelimitedSerdeFactory extends KsqlSerdeFactory {

  public KsqlDelimitedSerdeFactory() {
    super(Format.DELIMITED);
  }

  @Override
  public void validate(final ConnectSchema schema) {
    if (schema.type() != Type.STRUCT) {
      throw new IllegalArgumentException("DELIMITED format does not support unwrapping");
    }

    schema.fields().forEach(f -> SchemaWalker.visit(f.schema(), new SchemaValidator()));
  }

  @Override
  protected Serializer<Object> createSerializer(
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  ) {
    final Serializer<Object> serializer = new KsqlDelimitedSerializer();
    serializer.configure(Collections.emptyMap(), false);
    return serializer;
  }

  @Override
  protected Deserializer<Object> createDeserializer(
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final ProcessingLogger processingLogger
  ) {
    final Deserializer<Object> deserializer =
        new KsqlDelimitedDeserializer(schema, processingLogger);

    deserializer.configure(Collections.emptyMap(), false);

    return deserializer;
  }

  private static class SchemaValidator implements SchemaWalker.Visitor<Void, Void> {

    public Void visitPrimitive(final Schema schema) {
      // Primitive types are allowed.
      return null;
    }

    public Void visitBytes(final Schema schema) {
      // Decimal type is allowed.
      return null;
    }

    public Void visitSchema(final Schema schema) {
      final String typeString = schema.type() == Type.BYTES ? "DECIMAL" : schema.type().toString();
      throw new KsqlException("The '" + Format.DELIMITED
          + "' format does not support type '" + typeString + "'");
    }
  }
}
