package io.confluent.ksql.serde.protobuf;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ProtobufFormatTest {
  private ProtobufFormat format;
  private Map<String, String> formatProps;

  @Before
  public void setUp() {
    format = new ProtobufFormat();
    formatProps = new HashMap<>();
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
