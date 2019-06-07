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

package io.confluent.ksql.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TableElement;
import java.io.IOException;
import java.util.HashMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class AvroSchemaInferenceTest {
  @Test
  public void shouldInferIntAsInteger() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.builder().intType(),
        Schema.OPTIONAL_INT32_SCHEMA
    );
  }

  @Test
  public void shouldInferLongAsLong() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.builder().longType(),
        Schema.OPTIONAL_INT64_SCHEMA
    );
  }

  @Test
  public void shouldInferBooleanAsBoolean() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.builder().booleanType(),
        Schema.OPTIONAL_BOOLEAN_SCHEMA
    );
  }

  @Test
  public void shouldInferStringAsString() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.builder().stringType(),
        Schema.OPTIONAL_STRING_SCHEMA
    );
  }

  @Test
  public void shouldInferFloatAsDouble() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.builder().floatType(),
        Schema.OPTIONAL_FLOAT64_SCHEMA
    );
  }

  @Test
  public void shouldInferDoubleAsDouble() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.builder().doubleType(),
        Schema.OPTIONAL_FLOAT64_SCHEMA
    );
  }

  @Test
  public void shouldInferArrayAsArray() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.builder().array().items(
            org.apache.avro.SchemaBuilder.builder().longType()
        ),
        SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build()
    );
  }

  @Test
  public void shouldInferMapAsMap() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.builder().map().values(
            org.apache.avro.SchemaBuilder.builder().intType()
        ),
        SchemaBuilder.map(
            Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA
        ).optional().build()
    );
  }

  @Test
  public void shouldInferRecordAsAStruct() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.record("inner_record").fields()
            .name("inner1").type().intType().noDefault()
            .name("inner2").type().stringType().noDefault()
            .endRecord(),
        SchemaBuilder.struct()
            .field("INNER1", Schema.OPTIONAL_INT32_SCHEMA)
            .field("INNER2", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build()
    );
  }

  @Test
  public void shouldInferRecordWithOptionalField() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.record("inner_record").fields()
            .name("inner1").type().optional().intType()
            .name("inner2").type().optional().stringType()
            .endRecord(),
        SchemaBuilder.struct()
            .field("INNER1", Schema.OPTIONAL_INT32_SCHEMA)
            .field("INNER2", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build()
    );
  }

  @Test
  public void shouldInferOptionalField() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.unionOf().nullType().and().intType().endUnion(),
        Schema.OPTIONAL_INT32_SCHEMA
    );
  }

  @Test
  public void shouldInferEnumAsString() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.enumeration("foo").symbols("A", "B", "C"),
        Schema.OPTIONAL_STRING_SCHEMA
    );
  }

  @Test
  public void shouldInferDateAsIntegeer() {
    shouldInferType(
        org.apache.avro.LogicalTypes.date().addToSchema(
            org.apache.avro.SchemaBuilder.builder().intType()
        ),
        Schema.OPTIONAL_INT32_SCHEMA
    );
  }

  @Test
  public void shouldInferTimeMillisAsInteger() {
    shouldInferType(
        org.apache.avro.LogicalTypes.timeMillis().addToSchema(
            org.apache.avro.SchemaBuilder.builder().intType()
        ),
        Schema.OPTIONAL_INT32_SCHEMA
    );
  }

  @Test
  public void shouldInferTimeMicrosAsBigint() {
    shouldInferType(
        org.apache.avro.LogicalTypes.timeMicros().addToSchema(
            org.apache.avro.SchemaBuilder.builder().longType()
        ),
        Schema.OPTIONAL_INT64_SCHEMA
    );
  }

  @Test
  public void shouldInferTimestampMillisAsBigint() {
    shouldInferType(
        org.apache.avro.LogicalTypes.timestampMillis().addToSchema(
            org.apache.avro.SchemaBuilder.builder().longType()
        ),
        Schema.OPTIONAL_INT64_SCHEMA
    );

  }

  @Test
  public void shouldInferTimestampMicrosAsBigint() {
    shouldInferType(
        org.apache.avro.LogicalTypes.timestampMicros().addToSchema(
            org.apache.avro.SchemaBuilder.builder().longType()
        ),
        Schema.OPTIONAL_INT64_SCHEMA
    );
  }

  @Test
  public void shouldIgnoreUnion() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.unionOf().intType().and().stringType().endUnion(),
        null
    );
  }

  @Test
  public void shouldIgnoreFixed() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.fixed("fixed_field").size(32),
        null
    );
  }

  @Test
  public void shouldIgnoreBytes() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.builder().bytesType(),
        null
    );
  }

  @Test
  public void shouldInferInt8AsInteger() {
    shouldInferConnectType(Schema.INT8_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA);
  }

  @Test
  public void shouldInferInt16AsInteger() {
    shouldInferConnectType(Schema.INT16_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA);
  }

  @Test
  public void shouldInferOptional() {
    shouldInferConnectType(Schema.OPTIONAL_INT32_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA);
  }

  @Test
  public void shouldInferConnectMapWithOptionalStringKey() {
    shouldInferConnectType(
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build(),
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build()
    );
  }

  @Test
  public void shouldIgnoreConnectMapWithNonStringKey() {
    shouldInferConnectType(
        SchemaBuilder.map(Schema.OPTIONAL_INT32_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA),
        null
    );
  }

  @Test
  public void shouldInferComplexConnectSchema() {
    final Schema arrayInner = SchemaBuilder.struct()
        .field("arrayInner1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("arrayInner2", Schema.INT64_SCHEMA)
        .name("arrayInner")
        .build();
    final Schema mapInner = SchemaBuilder.struct()
        .field("mapInner1", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("mapInner2", Schema.STRING_SCHEMA)
        .name("mapInner")
        .build();
    final Schema structInner2 = SchemaBuilder.struct()
        .field("structInner2_1", Schema.BOOLEAN_SCHEMA)
        .field("structInner2_2", Schema.OPTIONAL_INT32_SCHEMA)
        .name("structInner2")
        .optional()
        .build();
    final Schema structInner1 = SchemaBuilder.struct()
        .field("structInner1_1", Schema.STRING_SCHEMA)
        .field("structInner1_2", structInner2)
        .name("structInner1")
        .build();
    final Schema connectSchema = SchemaBuilder.struct()
        .field("primitive", Schema.INT32_SCHEMA)
        .field("array", arrayInner)
        .field("map", mapInner)
        .field("struct", structInner1)
        .build();

    final Schema ksqlArrayInner = SchemaBuilder.struct()
        .field("ARRAYINNER1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("ARRAYINNER2", Schema.OPTIONAL_INT64_SCHEMA)
        .optional()
        .build();
    final Schema ksqlMapInner = SchemaBuilder.struct()
        .field("MAPINNER1", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("MAPINNER2", Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .build();
    final Schema ksqlStructInner2 = SchemaBuilder.struct()
        .field("STRUCTINNER2_1", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("STRUCTINNER2_2", Schema.OPTIONAL_INT32_SCHEMA)
        .optional()
        .build();
    final Schema ksqlStructInner1 = SchemaBuilder.struct()
        .field("STRUCTINNER1_1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("STRUCTINNER1_2", ksqlStructInner2)
        .optional()
        .build();
    final Schema ksqlSchema = SchemaBuilder.struct()
        .field("PRIMITIVE", Schema.OPTIONAL_INT32_SCHEMA)
        .field("ARRAY", ksqlArrayInner)
        .field("MAP", ksqlMapInner)
        .field("STRUCT", ksqlStructInner1)
        .build();

    shouldInferSchema(
        new AvroData(1).fromConnectSchema(connectSchema),
        ksqlSchema);
  }

  private Schema getSchemaForDdlStatement(final AbstractStreamCreateStatement statement) {
    final SchemaBuilder builder = SchemaBuilder.struct();
    for (final TableElement tableElement : statement.getElements()) {
      builder.field(
          tableElement.getName(),
          TypeUtil.getTypeSchema(tableElement.getType())
      );
    }
    return builder.build();
  }

  private void shouldInferSchema(final org.apache.avro.Schema avroStreamSchema,
                                 final Schema ksqlStreamSchema) {
    final MetaStore metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();

    try {
      schemaRegistryClient.register("test-value", avroStreamSchema);
    } catch (IOException | RestClientException e) {
      throw new RuntimeException(e);
    }

    final String statementText
        = "CREATE STREAM TEST WITH (KAFKA_TOPIC='test', VALUE_FORMAT='avro');";
    final KsqlParser parser = new KsqlParser();
    final Statement statement = parser.buildAst(statementText, metaStore).get(0);

    final StatementWithSchema inferred
        = StatementWithSchema.forStatement(
        statement, statementText, new HashMap<>(), schemaRegistryClient);

    final Statement statementWithSchema
        = parser.buildAst(inferred.getStatementText(), metaStore).get(0);
    final Schema inferredSchema = getSchemaForDdlStatement(
        (AbstractStreamCreateStatement) statementWithSchema);
    assertThat(inferredSchema, equalTo(ksqlStreamSchema));
  }

  private void shouldInferType(final org.apache.avro.Schema avroSchema,
                               final Schema ksqlSchema) {
    final org.apache.avro.Schema avroStreamSchema
        = org.apache.avro.SchemaBuilder.record("stream")
        .fields()
        .name("field0").type(avroSchema).noDefault()
        .endRecord();
    final SchemaBuilder ksqlStreamSchemaBuilder = SchemaBuilder.struct();
    if (ksqlSchema != null) {
      ksqlStreamSchemaBuilder.field("FIELD0", ksqlSchema).build();
    }
    shouldInferSchema(avroStreamSchema, ksqlStreamSchemaBuilder.build());
  }

  private void shouldInferConnectType(final Schema connectSchema,
                                      final Schema ksqlSchema) {
    shouldInferType(
        new AvroData(1).fromConnectSchema(connectSchema),
        ksqlSchema
    );
  }
}
