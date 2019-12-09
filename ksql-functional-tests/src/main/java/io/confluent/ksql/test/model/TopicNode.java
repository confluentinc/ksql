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

package io.confluent.ksql.test.model;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import io.confluent.connect.avro.AvroData;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.test.TestFrameworkException;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.test.tools.Topic;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.test.utils.SerdeUtil;
import java.util.Optional;
import org.apache.avro.Schema;

public final class TopicNode {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String FORMAT_REPLACE_ERROR =
      "To use {FORMAT} in your topics please set the 'format' test case element";

  private final String name;
  private final String format;
  private final Optional<Schema> avroSchema;
  private final int numPartitions;
  private final int replicas;

  TopicNode(
      @JsonProperty("name") final String name,
      @JsonProperty("schema") final JsonNode schema,
      @JsonProperty("format") final String format,
      @JsonProperty("partitions") final Integer numPartitions,
      @JsonProperty("replicas") final Integer replicas
  ) {
    this.name = name == null ? "" : name;
    this.avroSchema = buildAvroSchema(requireNonNull(schema, "schema"));
    this.format = format == null ? "" : format;
    this.numPartitions = numPartitions == null ? 1 : numPartitions;
    this.replicas = replicas == null ? 1 : replicas;

    if (this.name.isEmpty()) {
      throw new InvalidFieldException("name", "empty or missing");
    }
  }

  public Topic build(final Optional<String> defaultFormat) {
    final String formatToUse = format
        .replace("{FORMAT}", defaultFormat.orElse(FORMAT_REPLACE_ERROR));

    final SerdeSupplier<?> keySerdeSupplier = SerdeUtil.getKeySerdeSupplier(
        KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
        LogicalSchema.builder()::build // Assume default STRING key for now.
    );

    final SerdeSupplier<?> valueSerdeSupplier = SerdeUtil.getSerdeSupplier(
        Format.of(formatToUse),
        this::logicalSchema
    );

    return new Topic(
        name,
        avroSchema,
        keySerdeSupplier,
        valueSerdeSupplier,
        numPartitions,
        replicas
    );
  }

  private LogicalSchema logicalSchema() {
    if (!avroSchema.isPresent()) {
      throw new TestFrameworkException("Test framework requires "
          + "the schema of any topic using format KAFKA");
    }

    final org.apache.kafka.connect.data.Schema valueSchema = new AvroData(1)
        .toConnectSchema(avroSchema.get());

    final SqlStruct valueType = (SqlStruct) SchemaConverters.connectToSqlConverter()
        .toSqlType(valueSchema);

    final Builder schemaBuilder = LogicalSchema.builder();

    valueType.fields().forEach(field -> schemaBuilder.valueColumn(
        ColumnName.of(field.name()),
        field.type()));

    return schemaBuilder.build();
  }

  private static Optional<Schema> buildAvroSchema(final JsonNode schema) {
    if (schema instanceof NullNode) {
      return Optional.empty();
    }

    try {
      final String schemaString = OBJECT_MAPPER.writeValueAsString(schema);
      final Schema.Parser parser = new Schema.Parser();
      return Optional.of(parser.parse(schemaString));
    } catch (final Exception e) {
      throw new InvalidFieldException("schema", "failed to parse", e);
    }
  }
}