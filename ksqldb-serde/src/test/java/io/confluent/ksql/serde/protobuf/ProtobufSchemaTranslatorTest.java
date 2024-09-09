/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.serde.protobuf;

import static io.confluent.connect.protobuf.ProtobufDataConfig.SCHEMAS_CACHE_SIZE_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class ProtobufSchemaTranslatorTest {

  private static Schema CONNECT_SCHEMA_WITH_NULLABLE_PRIMITIVES =
      new SchemaBuilder(Type.STRUCT)
          .field("optional_int32", Schema.OPTIONAL_INT32_SCHEMA)
          .field("optional_boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .field("optional_string", Schema.OPTIONAL_STRING_SCHEMA)
          .build();

  private static final ProtobufSchema SCHEMA_WITH_WRAPPED_PRIMITIVES =
      new ProtobufSchema("syntax = \"proto3\"; import 'google/protobuf/wrappers.proto'; message ConfluentDefault1 {google.protobuf.BoolValue c1 = 1; google.protobuf.Int32Value c2 = 2; google.protobuf.Int64Value c3 = 3; google.protobuf.DoubleValue c4 = 4; google.protobuf.StringValue c5 = 5;}");

  private ProtobufSchemaTranslator schemaTranslator;

  @Test
  public void shouldAddFullNameToConnectSchema() {
    // Given:
    schemaTranslator = new ProtobufSchemaTranslator(new ProtobufProperties(ImmutableMap.of(
        ProtobufProperties.FULL_SCHEMA_NAME, "io.test.proto.Customer"
    )));
    final ProtobufSchema protoSchema = new ProtobufSchema("syntax = \"proto3\";\n" +
        "package io.test.proto;\n" +
        "\n" +
        "message Customer {\n" +
        "  int32 ID = 1;\n" +
        "  CustomerAddress ADDRESS = 2;\n" +
        "  \n" +
        "  message CustomerAddress {\n" +
        "  \t\tstring STREET = 1;\n" +
        "  };\n" +
        "}");

    // When:
    final Schema connectSchema = schemaTranslator.toConnectSchema(protoSchema);
    final ParsedSchema parsedSchema = schemaTranslator.fromConnectSchema(connectSchema);

    // Then:
    assertThat(connectSchema.name(), is("io.test.proto.Customer"));
    assertThat(connectSchema.field("ADDRESS").schema().name(),
        is("io.test.proto.Customer.CustomerAddress"));
    assertThat(parsedSchema, is(protoSchema));
  }

  @Test
  public void shouldUnwrapPrimitives() {
    // Given:
    givenUnwrapPrimitives();

    // When:
    final Schema schema = schemaTranslator.toConnectSchema(SCHEMA_WITH_WRAPPED_PRIMITIVES);

    // Then:
    assertThat(schema.field("c1").schema().type(), is(Type.BOOLEAN));
    assertThat(schema.field("c2").schema().type(), is(Type.INT32));
    assertThat(schema.field("c3").schema().type(), is(Type.INT64));
    assertThat(schema.field("c4").schema().type(), is(Type.FLOAT64));
    assertThat(schema.field("c5").schema().type(), is(Type.STRING));
  }

  @Test
  public void shouldWrapPrimitives() {
    // Given:
    givenWrapPrimitives();

    // When:
    final Schema schema = schemaTranslator.toConnectSchema(SCHEMA_WITH_WRAPPED_PRIMITIVES);

    // Then:
    assertThat(schema.field("c1").schema().type(), is(Type.STRUCT));
    assertThat(schema.field("c1").schema().field("value").schema().type(), is(Type.BOOLEAN));
    assertThat(schema.field("c2").schema().type(), is(Type.STRUCT));
    assertThat(schema.field("c2").schema().field("value").schema().type(), is(Type.INT32));
    assertThat(schema.field("c3").schema().type(), is(Type.STRUCT));
    assertThat(schema.field("c3").schema().field("value").schema().type(), is(Type.INT64));
    assertThat(schema.field("c4").schema().type(), is(Type.STRUCT));
    assertThat(schema.field("c4").schema().field("value").schema().type(), is(Type.FLOAT64));
    assertThat(schema.field("c5").schema().type(), is(Type.STRUCT));
    assertThat(schema.field("c5").schema().field("value").schema().type(), is(Type.STRING));
  }

  @Test
  public void shouldKeepUnwrappingPrimitivesOnConfigure() {
    // Given:
    givenUnwrapPrimitives();

    // When:
    schemaTranslator.configure(ImmutableMap.of(SCHEMAS_CACHE_SIZE_CONFIG, 1));
    final Schema schema = schemaTranslator.toConnectSchema(SCHEMA_WITH_WRAPPED_PRIMITIVES);

    // Then:
    assertThat(schema.field("c1").schema().type(), is(Type.BOOLEAN));
    assertThat(schema.field("c2").schema().type(), is(Type.INT32));
    assertThat(schema.field("c3").schema().type(), is(Type.INT64));
    assertThat(schema.field("c4").schema().type(), is(Type.FLOAT64));
    assertThat(schema.field("c5").schema().type(), is(Type.STRING));
  }

  @Test
  public void shouldKeepWrappingPrimitivesOnConfigure() {
    // Given:
    givenWrapPrimitives();

    // When:
    schemaTranslator.configure(ImmutableMap.of(SCHEMAS_CACHE_SIZE_CONFIG, 1));
    final Schema schema = schemaTranslator.toConnectSchema(SCHEMA_WITH_WRAPPED_PRIMITIVES);

    // Then:
    assertThat(schema.field("c1").schema().type(), is(Type.STRUCT));
    assertThat(schema.field("c1").schema().field("value").schema().type(), is(Type.BOOLEAN));
    assertThat(schema.field("c2").schema().type(), is(Type.STRUCT));
    assertThat(schema.field("c2").schema().field("value").schema().type(), is(Type.INT32));
    assertThat(schema.field("c3").schema().type(), is(Type.STRUCT));
    assertThat(schema.field("c3").schema().field("value").schema().type(), is(Type.INT64));
    assertThat(schema.field("c4").schema().type(), is(Type.STRUCT));
    assertThat(schema.field("c4").schema().field("value").schema().type(), is(Type.FLOAT64));
    assertThat(schema.field("c5").schema().type(), is(Type.STRUCT));
    assertThat(schema.field("c5").schema().field("value").schema().type(), is(Type.STRING));
  }


  @Test
  public void shouldApplyNullableAsOptional() {
    // Given:
    givenNullableAsOptional();

    // When:
    final ParsedSchema schema = schemaTranslator.fromConnectSchema(CONNECT_SCHEMA_WITH_NULLABLE_PRIMITIVES);

    // Then:
    assertThat(schema.canonicalString(), is("syntax = \"proto3\";\n"
        + "\n"
        + "message ConnectDefault1 {\n"
        + "  optional int32 optional_int32 = 1;\n"
        + "  optional bool optional_boolean = 2;\n"
        + "  optional string optional_string = 3;\n"
        + "}\n"));
  }

  @Test
  public void shouldApplyNullableAsWrapper() {
    // Given:
    givenNullableAsWrapper();

    // When:
    final ParsedSchema schema = schemaTranslator.fromConnectSchema(CONNECT_SCHEMA_WITH_NULLABLE_PRIMITIVES);

    // Then:
    assertThat(schema.canonicalString(), is("syntax = \"proto3\";\n"
        + "\n"
        + "import \"google/protobuf/wrappers.proto\";\n"
        + "\n"
        + "message ConnectDefault1 {\n"
        + "  google.protobuf.Int32Value optional_int32 = 1;\n"
        + "  google.protobuf.BoolValue optional_boolean = 2;\n"
        + "  google.protobuf.StringValue optional_string = 3;\n"
        + "}\n"));
  }

  @Test
  public void shouldUsePlainPrimitivesIfNoNullableRepresentationIsSet() {
    // Given:
    givenNoNullableRepresentation();

    // When:
    final ParsedSchema schema = schemaTranslator.fromConnectSchema(CONNECT_SCHEMA_WITH_NULLABLE_PRIMITIVES);

    // Then:
    assertThat(schema.canonicalString(), is("syntax = \"proto3\";\n"
        + "\n"
        + "message ConnectDefault1 {\n"
        + "  int32 optional_int32 = 1;\n"
        + "  bool optional_boolean = 2;\n"
        + "  string optional_string = 3;\n"
        + "}\n"));
  }

  @Test
  public void shouldReturnParsedSchemaWithDefaultFullSchemaName() {
    // Given:
    givenSchemaFullName("ConnectDefault1");
    final Schema connectSchema =  SchemaBuilder.struct()
        .field("id", Schema.OPTIONAL_INT64_SCHEMA)
        .field("array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA))
        .field("map", SchemaBuilder.map(Schema.BYTES_SCHEMA, Schema.FLOAT64_SCHEMA))
        .field("struct", SchemaBuilder.struct()
            .field("c1", Schema.STRING_SCHEMA)
            .build())
        .build();

    // When:
    schemaTranslator.configure(ImmutableMap.of());
    final ParsedSchema parsedSchema = schemaTranslator.fromConnectSchema(connectSchema);

    // Then:
    assertThat(parsedSchema.name(), is("ConnectDefault1"));
    assertThat(((ProtobufSchema)parsedSchema).rawSchema().toSchema(), is(
        "// Proto schema formatted by Wire, do not edit.\n" +
            "// Source: \n" +
            "\n" +
            "syntax = \"proto3\";\n" +
            "\n" +
            "message ConnectDefault1 {\n" +
            "  int64 id = 1;\n" +
            "\n" +
            "  repeated string array = 2;\n" +
            "\n" +
            "  repeated ConnectDefault2Entry map = 3;\n" +
            "\n" +
            "  ConnectDefault3 struct = 4;\n" +
            "\n" +
            "  message ConnectDefault2Entry {\n" +
            "    bytes key = 1;\n" +
            "  \n" +
            "    double value = 2;\n" +
            "  }\n" +
            "\n" +
            "  message ConnectDefault3 {\n" +
            "    string c1 = 1;\n" +
            "  }\n" +
            "}\n"
    ));
  }

  @Test
  public void shouldReturnParsedSchemaWithFullSchemaName() {
    // Given:
    givenSchemaFullName("io.examples.Customer");
    final Schema connectSchema =  SchemaBuilder.struct()
        .field("id", Schema.OPTIONAL_INT64_SCHEMA)
        .field("array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA))
        .field("map", SchemaBuilder.map(Schema.BYTES_SCHEMA, Schema.FLOAT64_SCHEMA).name("InternalMap"))
        .field("struct", SchemaBuilder.struct()
            .field("c1", Schema.STRING_SCHEMA)
            .name("InternalStruct")
            .build())
        .build();

    // When:
    schemaTranslator.configure(ImmutableMap.of());
    final ParsedSchema parsedSchema = schemaTranslator.fromConnectSchema(connectSchema);

    // Then:
    assertThat(parsedSchema.name(), is("io.examples.Customer"));
    assertThat(((ProtobufSchema)parsedSchema).rawSchema().toSchema(), is(
        "// Proto schema formatted by Wire, do not edit.\n" +
            "// Source: \n" +
            "\n" +
            "syntax = \"proto3\";\n" +
            "\n" +
            "package io.examples;\n" +
            "\n" +
            "message Customer {\n" +
            "  int64 id = 1;\n" +
            "\n" +
            "  repeated string array = 2;\n" +
            "\n" +
            "  repeated InternalMapEntry map = 3;\n" +
            "\n" +
            "  InternalStruct struct = 4;\n" +
            "\n" +
            "  message InternalMapEntry {\n" +
            "    bytes key = 1;\n" +
            "  \n" +
            "    double value = 2;\n" +
            "  }\n" +
            "\n" +
            "  message InternalStruct {\n" +
            "    string c1 = 1;\n" +
            "  }\n" +
            "}\n"
    ));
  }

  private void givenUnwrapPrimitives() {
    schemaTranslator = new ProtobufSchemaTranslator(new ProtobufProperties(ImmutableMap.of(
        ProtobufProperties.UNWRAP_PRIMITIVES, ProtobufProperties.UNWRAP
    )));
  }

  private void givenWrapPrimitives() {
    schemaTranslator = new ProtobufSchemaTranslator(new ProtobufProperties(ImmutableMap.of()));
  }

  private void givenNullableAsOptional() {
    schemaTranslator = new ProtobufSchemaTranslator(new ProtobufProperties(ImmutableMap.of(
        ProtobufProperties.NULLABLE_REPRESENTATION, ProtobufProperties.NULLABLE_AS_OPTIONAL
    )));
  }

  private void givenNullableAsWrapper() {
    schemaTranslator = new ProtobufSchemaTranslator(new ProtobufProperties(ImmutableMap.of(
        ProtobufProperties.NULLABLE_REPRESENTATION, ProtobufProperties.NULLABLE_AS_WRAPPER
    )));
  }

  private void givenNoNullableRepresentation() {
    schemaTranslator = new ProtobufSchemaTranslator(new ProtobufProperties(ImmutableMap.of()));
  }

  private void givenSchemaFullName(final String fullSchemaName) {
    schemaTranslator = new ProtobufSchemaTranslator(new ProtobufProperties(ImmutableMap.of(
        ProtobufProperties.FULL_SCHEMA_NAME, fullSchemaName
    )));
  }
}