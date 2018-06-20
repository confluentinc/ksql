package io.confluent.ksql.serde.protobuf;

import com.google.protobuf.ByteString;
import io.confluent.ksql.GenericRow;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


public class KsqlProtobufDeserializerTest {

  private Schema protoASchema;
  private Schema protoBSchema;

  @Before
  public void before() {
    protoASchema = SchemaBuilder.struct()
      .field("name".toUpperCase(), Schema.OPTIONAL_STRING_SCHEMA)
      .field("message_id".toUpperCase(), Schema.OPTIONAL_INT32_SCHEMA)
      .build();

    protoBSchema = SchemaBuilder.struct()
      .field("double_field".toUpperCase(), Schema.FLOAT64_SCHEMA)
      .field("float_field".toUpperCase(), Schema.FLOAT32_SCHEMA)
      .field("int32_field".toUpperCase(), Schema.INT32_SCHEMA)
      .field("int64_field".toUpperCase(), Schema.INT64_SCHEMA)
      .field("uint32_field".toUpperCase(), Schema.INT32_SCHEMA)
      .field("uint64_field".toUpperCase(), Schema.INT64_SCHEMA)
      .field("sint32_field".toUpperCase(), Schema.INT32_SCHEMA)
      .field("sint64_field".toUpperCase(), Schema.INT64_SCHEMA)
      .field("fixed32_field".toUpperCase(), Schema.INT32_SCHEMA)
      .field("fixed64_field".toUpperCase(), Schema.INT64_SCHEMA)
      .field("sfixed32_field".toUpperCase(), Schema.INT32_SCHEMA)
      .field("sfixed64_field".toUpperCase(), Schema.INT64_SCHEMA)
      .field("bool_field".toUpperCase(), Schema.BOOLEAN_SCHEMA)
      .field("string_field".toUpperCase(), Schema.STRING_SCHEMA)
      .field("bytes_field".toUpperCase(), Schema.BYTES_SCHEMA)
      .field("custom_enum_field".toUpperCase(), Schema.STRING_SCHEMA)
      // Array field
      .field("array_field".toUpperCase(), SchemaBuilder.array(Schema.STRING_SCHEMA))
      // Struct field
      .field("embedded_field".toUpperCase(), SchemaBuilder.struct()
        .field("name".toUpperCase(), Schema.STRING_SCHEMA)
      )
      // Map field
      .field("map_field".toUpperCase(), SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA))
      .build();
  }

  /**
   * Simple smoke test deserializing a simple Protobuf "SampleProtoA".
   */
  @Test
  public void shouldDeserializeJsonCorrectly() {
    final String expectedName = "Stevie-san";
    final int expectedMessageId = 4321;

    // Create protobuf
    final SampleProtoAOuterClass.SampleProtoA sampleProtoA = SampleProtoAOuterClass.SampleProtoA.newBuilder()
      .setName(expectedName)
      .setMessageId(expectedMessageId)
      .build();

    // Serialize into bytes.
    final byte[] bytes = sampleProtoA.toByteArray();

    // Define properties.
    final Map<String, Object> config = new HashMap<>();
    config.put(KsqlProtobufTopicSerDe.CONFIG_PROTOBUF_CLASS, SampleProtoAOuterClass.SampleProtoA.class.getName());

    // Create & configure deserializer.
    final KsqlProtobufDeserializer ksqlProtobufDeserializer = new KsqlProtobufDeserializer(protoASchema, false);
    ksqlProtobufDeserializer.configure(config, false);

    // Deserialize our bytes into a generic row.
    final GenericRow genericRow = ksqlProtobufDeserializer.deserialize("Not-Relevant", bytes);

    // Validate result
    assertThat(genericRow.getColumns().size(), equalTo(2));
    assertThat((String) genericRow.getColumnValue(0), equalTo(expectedName));
    assertThat((Integer) genericRow.getColumnValue(1), equalTo(expectedMessageId));
  }

  /**
   * More complicated smoke test deserializing a Protobuf "SampleProtoB".
   * This protobuf (hopefully) contains all possible types of fields, validating that each
   * is handled appropriately.
   */
  @Test
  public void shouldDeserializeProtoBCorrectly() {
    final double expectedDouble = 123.424;
    final float expectedFloat = 44.4f;
    final int expectedInt32 = -32;
    final long expectedInt64 = (long) Integer.MAX_VALUE + 1;
    final int expectedUInt32 = 323;
    final long expectedUInt64 = (long) Integer.MAX_VALUE + 10;
    final int expectedSInt32 = 4499;
    final long expectedSInt64 = (long) Integer.MAX_VALUE + 421;
    final int expectedFixedInt32 = 234;
    final long expectedFixedInt64 = (long) Integer.MAX_VALUE + 234;
    final int expectedSFixedInt32 = 2344;
    final long expectedSFixedInt64 = (long) Integer.MAX_VALUE + 444;
    final boolean expectedBool = true;
    final String expectedString = "Bob";
    final byte[] expectedBytes = "Stevie-san".getBytes(StandardCharsets.UTF_8);
    final SampleProtoBOuterClass.SampleProtoB.CustomEnum expectedEnum = SampleProtoBOuterClass.SampleProtoB.CustomEnum.C;
    final String[] expectedArray = new String[] { "Value1", "Value2", "Value3" };
    final String expectedEmbeddedFieldName = "My Embedded Field Name";

    // Create protobuf
    final SampleProtoBOuterClass.SampleProtoB sampleProtoB = SampleProtoBOuterClass.SampleProtoB.newBuilder()
      .setDoubleField(expectedDouble)
      .setFloatField(expectedFloat)
      .setInt32Field(expectedInt32)
      .setInt64Field(expectedInt64)
      .setUint32Field(expectedUInt32)
      .setUint64Field(expectedUInt64)
      .setSint32Field(expectedSInt32)
      .setSint64Field(expectedSInt64)
      .setFixed32Field(expectedFixedInt32)
      .setFixed64Field(expectedFixedInt64)
      .setSfixed32Field(expectedSFixedInt32)
      .setSfixed64Field(expectedSFixedInt64)
      .setBoolField(expectedBool)
      .setStringField(expectedString)
      .setBytesField(ByteString.copyFrom(expectedBytes))
      // Enum Field
      .setCustomEnumField(expectedEnum)
      // Array Field
      .addAllArrayField(Arrays.asList(expectedArray))
      // Message field.
      .setEmbeddedField(SampleProtoBOuterClass.SampleProtoB.Embedded.newBuilder()
        .setName(expectedEmbeddedFieldName)
        .build()
      )
      .putMapField("Key1", 1)
      .putMapField("Key2", 2)
      .putMapField("Key3", 3)
      // Map field
      .build();

    // Serialize into bytes.
    final byte[] bytes = sampleProtoB.toByteArray();

    // Define properties.
    final Map<String, Object> config = new HashMap<>();
    config.put(KsqlProtobufTopicSerDe.CONFIG_PROTOBUF_CLASS, SampleProtoBOuterClass.SampleProtoB.class.getName());

    // Create & configure deserializer.
    final KsqlProtobufDeserializer ksqlProtobufDeserializer = new KsqlProtobufDeserializer(protoBSchema, false);
    ksqlProtobufDeserializer.configure(config, false);

    // Deserialize our bytes into a generic row.
    final GenericRow genericRow = ksqlProtobufDeserializer.deserialize("Not-Relevant", bytes);

    // Validate results for primitive types.
    assertThat(genericRow.getColumns().size(), equalTo(19));
    assertThat((Double) genericRow.getColumnValue(0), equalTo(expectedDouble));
    assertThat((Float) genericRow.getColumnValue(1), equalTo(expectedFloat));
    assertThat((Integer) genericRow.getColumnValue(2), equalTo(expectedInt32));
    assertThat((Long) genericRow.getColumnValue(3), equalTo(expectedInt64));
    assertThat((Integer) genericRow.getColumnValue(4), equalTo(expectedUInt32));
    assertThat((Long) genericRow.getColumnValue(5), equalTo(expectedUInt64));
    assertThat((Integer) genericRow.getColumnValue(6), equalTo(expectedSInt32));
    assertThat((Long) genericRow.getColumnValue(7), equalTo(expectedSInt64));
    assertThat((Integer) genericRow.getColumnValue(8), equalTo(expectedFixedInt32));
    assertThat((Long) genericRow.getColumnValue(9), equalTo(expectedFixedInt64));
    assertThat((Integer) genericRow.getColumnValue(10), equalTo(expectedSFixedInt32));
    assertThat((Long) genericRow.getColumnValue(11), equalTo(expectedSFixedInt64));
    assertThat((Boolean) genericRow.getColumnValue(12), equalTo(expectedBool));
    assertThat((String) genericRow.getColumnValue(13), equalTo(expectedString));
    assertThat((byte[]) genericRow.getColumnValue(14), equalTo(expectedBytes));

    // Validate ENUM
    assertThat((String) genericRow.getColumnValue(15), equalTo(expectedEnum.getValueDescriptor().getName()));

    // Validate Array
    final List<String> arrayValues = genericRow.getColumnValue(16);
    assertThat(arrayValues.toArray(), equalTo(expectedArray));

    // Validate embedded message
    final List<Object> embeddedMessage = genericRow.getColumnValue(17);
    assertThat(embeddedMessage.size(), equalTo(1));
    assertThat(embeddedMessage.get(0), equalTo(expectedEmbeddedFieldName));

    // Validate Map
    final Map<Object, Object> mapValues = genericRow.getColumnValue(18);
    assertThat(mapValues.size(), equalTo(3));
    assertThat(mapValues.containsKey("Key1"), equalTo(true));
    assertThat(mapValues.get("Key1"), equalTo(1));
    assertThat(mapValues.containsKey("Key2"), equalTo(true));
    assertThat(mapValues.get("Key2"), equalTo(2));
    assertThat(mapValues.containsKey("Key3"), equalTo(true));
    assertThat(mapValues.get("Key3"), equalTo(3));
  }
}