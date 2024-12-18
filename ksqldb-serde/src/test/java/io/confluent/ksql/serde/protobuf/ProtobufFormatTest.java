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

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ProtobufFormatTest {
  private ProtobufFormat format;

  @Before
  public void setUp() {
    format = new ProtobufFormat();
  }

  @Test
  public void shouldReturnSchemaNamesFromMultipleSchemaDefinitionsWithPackageName() {
    // Given
    final ProtobufSchema protoSchema = new ProtobufSchema(""
        + "syntax = \"proto3\"; "
        + "package examples.proto; "
        + "message ProtobufKey1 {uint32 k1 = 1;} "
        + "message ProtobufKey2 {string k1 = 1;}"
    );

    // When
    final List<String> schemaNames = format.schemaFullNames(protoSchema);

    // Then
    assertThat(schemaNames, equalTo(ImmutableList.of(
        "examples.proto.ProtobufKey1",
        "examples.proto.ProtobufKey2"
    )));
  }

  @Test
  public void shouldReturnSchemaNamesFromMultipleSchemaDefinitionsWithoutPackageName() {
    // Given
    final ProtobufSchema protoSchema = new ProtobufSchema(""
        + "syntax = \"proto3\"; "
        + "message ProtobufKey1 {uint32 k1 = 1;} "
        + "message ProtobufKey2 {string k1 = 1;}"
    );

    // When
    final List<String> schemaNames = format.schemaFullNames(protoSchema);

    // Then
    assertThat(schemaNames, equalTo(ImmutableList.of("ProtobufKey1", "ProtobufKey2")));
  }
}
