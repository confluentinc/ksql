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
import io.confluent.ksql.schema.connect.SchemaWalker;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.Delimiter;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.testing.EffectivelyImmutable;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.commons.csv.CSVFormat;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;


@Immutable
public class KsqlDelimitedSerdeFactory implements KsqlSerdeFactory {

  private static final Delimiter DEFAULT_DELIMITER = Delimiter.of(',');

  @EffectivelyImmutable
  private final CSVFormat csvFormat;

  public KsqlDelimitedSerdeFactory(final Optional<Delimiter> delimiter) {
    this.csvFormat =
        CSVFormat.DEFAULT.withDelimiter(delimiter.orElse(DEFAULT_DELIMITER).getDelimiter());
  }

  @Override
  public void validate(final PersistenceSchema schema) {
    final ConnectSchema connectSchema = schema.serializedSchema();
    if (connectSchema.type() != Type.STRUCT) {
      throw new IllegalArgumentException("DELIMITED format does not support unwrapping");
    }

    connectSchema.fields().forEach(f -> SchemaWalker.visit(f.schema(), new SchemaValidator()));
  }

  @Override
  public Serde<Object> createSerde(
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  ) {
    validate(schema);

    return Serdes.serdeFrom(
        new KsqlDelimitedSerializer(csvFormat),
        new KsqlDelimitedDeserializer(schema, csvFormat)
    );
  }

  private static class SchemaValidator implements SchemaWalker.Visitor<Void, Void> {

    public Void visitPrimitive(final Schema schema) {
      // Primitive types are allowed.
      return null;
    }

    public Void visitBytes(final Schema schema) {
      if (!DecimalUtil.isDecimal(schema)) {
        visitSchema(schema);
      }
      return null;
    }

    public Void visitSchema(final Schema schema) {
      throw new KsqlException("The '" + FormatFactory.DELIMITED.name()
          + "' format does not support type '" + schema.type().toString() + "'");
    }
  }
}
