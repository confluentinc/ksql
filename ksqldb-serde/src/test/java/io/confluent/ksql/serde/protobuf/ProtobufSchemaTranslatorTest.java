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
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.junit.Test;

public class ProtobufSchemaTranslatorTest {

  private static final ProtobufSchema SCHEMA_WITH_WRAPPED_PRIMITIVES =
      new ProtobufSchema("syntax = \"proto3\"; import 'google/protobuf/wrappers.proto'; message ConfluentDefault1 {google.protobuf.BoolValue c1 = 1; google.protobuf.Int32Value c2 = 2; google.protobuf.Int64Value c3 = 3; google.protobuf.DoubleValue c4 = 4; google.protobuf.StringValue c5 = 5;}");

  private ProtobufSchemaTranslator schemaTranslator;

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

  private void givenUnwrapPrimitives() {
    schemaTranslator = new ProtobufSchemaTranslator(new ProtobufProperties(ImmutableMap.of(
        ProtobufProperties.UNWRAP_PRIMITIVES, ProtobufProperties.UNWRAP
    )));
  }

  private void givenWrapPrimitives() {
    schemaTranslator = new ProtobufSchemaTranslator(new ProtobufProperties(ImmutableMap.of()));
  }

}