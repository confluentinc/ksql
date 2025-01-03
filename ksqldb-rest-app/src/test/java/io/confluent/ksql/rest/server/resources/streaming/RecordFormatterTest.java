/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources.streaming;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.ksql.rest.server.resources.streaming.RecordFormatter.Deserializers;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.SessionWindowedSerializer;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.internals.SessionKeySchema;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(Enclosed.class)
public class RecordFormatterTest {

  private static final String TOPIC_NAME = "some-topic";

  @RunWith(MockitoJUnitRunner.class)
  public static class MainClassTest {

    private static final Bytes KEY_BYTES = Bytes.wrap("the key".getBytes(UTF_8));
    private static final Bytes VALUE_BYTES = Bytes.wrap("the value".getBytes(UTF_8));

    @Mock
    private SchemaRegistryClient schemaRegistryClient;
    @Mock
    private Deserializers keyDeserializers;
    @Mock
    private Deserializers valueDeserializers;

    private RecordFormatter formatter;
    private long timestamp = 1588344313111L; // 1st of May 2020, 14:45:13:111 UTC

    @Before
    public void setUp() {
      when(keyDeserializers.getPossibleFormats()).thenReturn(ImmutableList.of("key-format"));
      when(valueDeserializers.getPossibleFormats()).thenReturn(ImmutableList.of("value-format"));

      formatter = new RecordFormatter(
          keyDeserializers,
          valueDeserializers
      );
    }

    @Test
    public void shouldStartWithUnknownKeyFormat() {
      // Given:
      formatter = new RecordFormatter(schemaRegistryClient, TOPIC_NAME);

      // Then:
      assertThat(formatter.getPossibleKeyFormats(), is(
          ImmutableList.of("¯\\_(ツ)_/¯ - no data processed")
      ));
    }

    @Test
    public void shouldStartWithUnknownValueFormat() {
      // Given:
      formatter = new RecordFormatter(schemaRegistryClient, TOPIC_NAME);

      // Then:
      assertThat(formatter.getPossibleValueFormats(), is(
          ImmutableList.of("¯\\_(ツ)_/¯ - no data processed")
      ));
    }

    @Test
    public void shouldStayWithUnknownKeyFormatIfProcessingNullKeys() {
      // Given:
      formatter = new RecordFormatter(schemaRegistryClient, TOPIC_NAME);

      // When:
      formatter.format(consumerRecords(null, VALUE_BYTES));

      // Then:
      assertThat(formatter.getPossibleKeyFormats(), is(
          ImmutableList.of("¯\\_(ツ)_/¯ - no data processed")
      ));
    }

    @Test
    public void shouldStayWithUnknownKeyFormatIfProcessingNullValues() {
      // Given:
      formatter = new RecordFormatter(schemaRegistryClient, TOPIC_NAME);

      // When:
      formatter.format(consumerRecords(KEY_BYTES, null));

      // Then:
      assertThat(formatter.getPossibleValueFormats(), is(
          ImmutableList.of("¯\\_(ツ)_/¯ - no data processed")
      ));
    }

    @Test
    public void shouldFormat() {
      // When:
      formatter.format(consumerRecords(KEY_BYTES, VALUE_BYTES));

      // Then:
      verify(keyDeserializers).format(KEY_BYTES);
      verify(valueDeserializers).format(VALUE_BYTES);
    }

    @Test
    public void shouldFormatNulls() {
      // When:
      formatSingle(consumerRecord(null, null));

      // Then:
      verify(keyDeserializers).format(null);
      verify(valueDeserializers).format(null);
    }

    @Test
    public void shouldFormatNullsBytes() {
      // Given:
      final Bytes nullBytes = new Bytes(null);

      // When:
      formatSingle(consumerRecord(nullBytes, nullBytes));

      // Then:
      verify(keyDeserializers).format(nullBytes);
      verify(valueDeserializers).format(nullBytes);
    }

    @Test
    public void shouldFormatRowTime() {
      // When:
      final String formatted = formatSingle(consumerRecord(null, null));

      // Then:
      assertThat(formatted, containsString("rowtime: 2020/05/01 14:45:13.111 Z, "));
    }

    @Test
    public void shouldFormatPartition() {
      // When:
      final String formatted = formatSingle(consumerRecord(null, null));

      // Then:
      assertThat(formatted, containsString("partition: 1"));
    }

    @Test
    public void shouldFormatNoRowTime() {
      // Given:
      timestamp = ConsumerRecord.NO_TIMESTAMP;

      // When:
      final String formatted = formatSingle(consumerRecord(null, null));

      // Then:
      assertThat(formatted, containsString("rowtime: N/A, "));
    }

    @Test
    public void shouldReturnPossibleKeyFormats() {
      // Given:
      final ImmutableList<String> expected = ImmutableList.of("f0", "f1");
      when(keyDeserializers.getPossibleFormats()).thenReturn(expected);

      // Then:
      assertThat(formatter.getPossibleKeyFormats(), is(expected));
    }

    @Test
    public void shouldReturnPossibleValueFormats() {
      // Given:
      final ImmutableList<String> expected = ImmutableList.of("f0", "f1");
      when(valueDeserializers.getPossibleFormats()).thenReturn(expected);

      // Then:
      assertThat(formatter.getPossibleValueFormats(), is(expected));
    }

    private String formatSingle(final ConsumerRecord<Bytes, Bytes> consumerRecord) {
      final List<String> result = formatter.format(ImmutableList.of(consumerRecord));
      assertThat(result, hasSize(1));
      return result.get(0);
    }

    @SuppressWarnings("SameParameterValue")
    private Iterable<ConsumerRecord<Bytes, Bytes>> consumerRecords(
        final Bytes keyBytes,
        final Bytes valueBytes
    ) {
      return ImmutableList.of(consumerRecord(keyBytes, valueBytes));
    }

    private ConsumerRecord<Bytes, Bytes> consumerRecord(
        final Bytes keyBytes,
        final Bytes valueBytes
    ) {
      return new ConsumerRecord<>(
          TOPIC_NAME,
          1,
          1,
          timestamp,
          TimestampType.CREATE_TIME,
          1,
          1,
          keyBytes,
          valueBytes,
          new RecordHeaders(),
          Optional.empty()
      );
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class DeserializersTest {

    private static final Random RNG = new Random();

    private static final Schema AVRO_SCHEMA = parseAvroSchema("{" +
        "    \"type\": \"record\"," +
        "    \"name\": \"myrecord\"," +
        "    \"fields\": [" +
        "        { \"name\": \"str1\", \"type\": \"string\" }" +
        "    ]" +
        "}");

    private static final String JSON_OBJECT = "{\"a\":1}";
    private static final String JSON_ARRAY = "[10,22,44]";
    private static final String JSON_TEXT = "\"Yes this is valid json\"";
    private static final String JSON_NUMBER = "24.987";
    private static final String JSON_BOOLEAN = "true";
    private static final String JSON_NULL = "null";
    private static final List<String> VALID_JSON = ImmutableList.of(
        JSON_OBJECT,
        JSON_ARRAY,
        JSON_TEXT,
        JSON_NUMBER,
        JSON_BOOLEAN,
        JSON_NULL
    );

    private static final List<Bytes> NULL_VARIANTS;

    static {
      final List<Bytes> nullVariants = new ArrayList<>();
      nullVariants.add(new Bytes(null));
      nullVariants.add(null);
      NULL_VARIANTS = Collections.unmodifiableList(nullVariants);
    }

    private static final int SERIALIZED_INT_SIZE = 4;
    private static final int SERIALIZED_BIGINT_SIZE = 8;
    private static final int SERIALIZED_DOUBLE_SIZE = 8;

    private static final Window TIME_WINDOW = new TimeWindow(1234567890123L, 1234567899999L);
    private static final Window SESSION_WINDOW = new SessionWindow(1534567890123L, 1534567899999L);

    private static final GenericRecord AVRO_RECORD = avroRecord();
    private static final Bytes SERIALIZED_AVRO_RECORD = serialize(AVRO_RECORD, avroSerializer());
    private static final Bytes SERIALIZED_TIME_WINDOWED_AVRO_RECORD = serialize(
        new Windowed<>(AVRO_RECORD, TIME_WINDOW),
        new TimeWindowedSerializer<>(avroSerializer())
    );
    private static final Bytes SERIALIZED_SESSION_WINDOWED_AVRO_RECORD = serialize(
        new Windowed<>(AVRO_RECORD, SESSION_WINDOW),
        new SessionWindowedSerializer<>(avroSerializer())
    );

    private static final ProtobufSchema PROTOBUF_SCHEMA = new ProtobufSchema(
        "syntax = \"proto3\"; message MyRecord {string str1 = 1; string str2 = 2;}");
    private static final Message PROTOBUF_RECORD = protobufRecord();
    private static final Bytes SERIALIZED_PROTOBUF_RECORD = serialize(PROTOBUF_RECORD, protobufSerializer());
    private static final Bytes SERIALIZED_TIME_WINDOWED_PROTOBUF_RECORD = serialize(
        new Windowed<>(PROTOBUF_RECORD, TIME_WINDOW),
        new TimeWindowedSerializer<>(protobufSerializer())
    );
    private static final Bytes SERIALIZED_SESSION_WINDOWED_PROTOBUF_RECORD = serialize(
        new Windowed<>(PROTOBUF_RECORD, SESSION_WINDOW),
        new SessionWindowedSerializer<>(protobufSerializer())
    );

    private static final JsonSchema JSON_SCHEMA = new JsonSchema(
        "{\"str1\": \"string\", \"str2\": \"string\"}");
    private static final Object JSON_SR_RECORD = ImmutableMap.of("str1", "My first string", "str2", "My second string");
    private static final Bytes SERIALIZED_JSON_SR_RECORD = serialize(JSON_SR_RECORD, jsonSrSerializer());
    private static final Bytes SERIALIZED_TIME_WINDOWED_JSON_SR_RECORD = serialize(
        new Windowed<>(JSON_SR_RECORD, TIME_WINDOW),
        new TimeWindowedSerializer<>(jsonSrSerializer())
    );
    private static final Bytes SERIALIZED_SESSION_WINDOWED_JSON_SR_RECORD = serialize(
        new Windowed<>(JSON_SR_RECORD, SESSION_WINDOW),
        new SessionWindowedSerializer<>(jsonSrSerializer())
    );

    private static final int KAFKA_INT = 24;
    private static final Bytes SERIALIZED_KAFKA_INT = serialize(KAFKA_INT, new IntegerSerializer());
    private static final Bytes SERIALIZED_TIME_WINDOWED_KAFKA_INT = serialize(
        new Windowed<>(KAFKA_INT, TIME_WINDOW),
        WindowedSerdes.timeWindowedSerdeFrom(Integer.class, TIME_WINDOW.end() - TIME_WINDOW.start()).serializer()
    );
    private static final Bytes SERIALIZED_SESSION_WINDOWED_KAFKA_INT = serialize(
        new Windowed<>(KAFKA_INT, SESSION_WINDOW),
        WindowedSerdes.sessionWindowedSerdeFrom(Integer.class).serializer()
    );

    private static final long KAFKA_BIGINT = 199L;
    private static final Bytes SERIALIZED_KAFKA_BIGINT = serialize(KAFKA_BIGINT,
        new LongSerializer()
    );
    private static final Bytes SERIALIZED_TIME_WINDOWED_KAFKA_BIGINT = serialize(
        new Windowed<>(KAFKA_BIGINT, TIME_WINDOW),
        WindowedSerdes.timeWindowedSerdeFrom(Long.class, TIME_WINDOW.end() - TIME_WINDOW.start()).serializer()
    );
    private static final Bytes SERIALIZED_SESSION_WINDOWED_KAFKA_BIGINT = serialize(
        new Windowed<>(KAFKA_BIGINT, SESSION_WINDOW),
        WindowedSerdes.sessionWindowedSerdeFrom(Long.class).serializer()
    );

    private static final double KAFKA_DOUBLE = 24.199d;
    private static final Bytes SERIALIZED_KAFKA_DOUBLE = serialize(
        KAFKA_DOUBLE,
        new DoubleSerializer()
    );
    private static final Bytes SERIALIZED_TIME_WINDOWED_KAFKA_DOUBLE = serialize(
        new Windowed<>(KAFKA_DOUBLE, TIME_WINDOW),
        WindowedSerdes.timeWindowedSerdeFrom(Double.class, TIME_WINDOW.end() - TIME_WINDOW.start()).serializer()
    );
    private static final Bytes SERIALIZED_SESSION_WINDOWED_KAFKA_DOUBLE = serialize(
        new Windowed<>(KAFKA_DOUBLE, SESSION_WINDOW),
        WindowedSerdes.sessionWindowedSerdeFrom(Double.class).serializer()
    );

    private static final String KAFKA_STRING = "κόσμε";
    private static final Bytes SERIALIZED_INVALID_UTF8 = Bytes.wrap(
        new byte[]{-1, -1, 'i', 's', ' ', 'i', 'n', 'v', 'a', 'l', 'i', 'd', -1, -1}
    );
    private static final Bytes SERIALIZED_KAFKA_STRING = serialize(
        KAFKA_STRING,
        new StringSerializer()
    );
    private static final Bytes SERIALIZED_TIME_WINDOWED_KAFKA_STRING = serialize(
        new Windowed<>(KAFKA_STRING, TIME_WINDOW),
        WindowedSerdes.timeWindowedSerdeFrom(String.class, TIME_WINDOW.end() - TIME_WINDOW.start()).serializer()
    );
    private static final Bytes SERIALIZED_SESSION_WINDOWED_KAFKA_STRING = serialize(
        new Windowed<>(KAFKA_STRING, SESSION_WINDOW),
        WindowedSerdes.sessionWindowedSerdeFrom(String.class).serializer()
    );

    @Mock
    private SchemaRegistryClient schemaRegistryClient;

    private Deserializers deserializers;

    @Before
    public void setUp() {
      deserializers = new Deserializers(TOPIC_NAME, schemaRegistryClient, true);
    }

    @Test
    public void shouldNotExcludeAnyThingOnNulls() {
      // Given:
      final List<String> expected = ImmutableList.copyOf(deserializers.getPossibleFormats());

      NULL_VARIANTS.forEach(nullVariant -> {
        // When:
        deserializers.format(nullVariant);

        // Then:
        assertThat(deserializers.getPossibleFormats(), is(expected));
      });
    }

    @Test
    public void shouldExcludeFixedSizeFormatsWhereSizeDoesNotMatch() {
      // When:
      deserializers.format(getBytes(2));

      // Then:
      assertThat(deserializers.getPossibleFormats(), notHasItems(
          "KAFKA_INT", "SESSION(KAFKA_INT)", "TUMBLING(KAFKA_INT)", "HOPPING(KAFKA_INT)",
          "KAFKA_BIGINT", "SESSION(KAFKA_BIGINT)", "TUMBLING(KAFKA_BIGINT)",
          "HOPPING(KAFKA_BIGINT)",
          "KAFKA_DOUBLE", "SESSION(KAFKA_DOUBLE)", "TUMBLING(KAFKA_DOUBLE)", "HOPPING(KAFKA_DOUBLE)"
      ));
    }

    @Test
    public void shouldExcludeSessionWindowedOnInvalidWindows() {
      // Given:
      final Bytes invalidAsEndTimeBeforeStart = SessionKeySchema
          .toBinary(getBytes(4), 100, 99);

      // When:
      deserializers.format(invalidAsEndTimeBeforeStart);

      // Then:
      assertThat(deserializers.getPossibleFormats(), notHasItems(
          "SESSION(AVRO)",
          "SESSION(JSON)",
          "SESSION(KAFKA_INT)",
          "SESSION(KAFKA_BIGINT)",
          "SESSION(KAFKA_DOUBLE)"
      ));
    }

    @Test
    public void shouldExcludeAvroNoSchema() {
      // Given:
      // not: givenAvroSchemaRegistered();

      // When:
      deserializers.format(SERIALIZED_AVRO_RECORD);

      // Then:
      assertThat(deserializers.getPossibleFormats(), not(hasItem("AVRO")));
    }

    @Test
    public void shouldNotExcludeAvroOnValidAvro() {
      // Given:
      givenAvroSchemaRegistered();

      // When:
      deserializers.format(SERIALIZED_AVRO_RECORD);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItems(
          "AVRO"
      ));
    }

    @Test
    public void shouldFormatValidAvro() {
      // Given:
      givenAvroSchemaRegistered();

      // When:
      final String formatted = deserializers.format(SERIALIZED_AVRO_RECORD);

      // Then:
      assertThat(formatted, is("{\"str1\": \"My first string\"}"));
    }

    @Test
    public void shouldNotExcludeTimeWindowedAvroOnValidTimeWindowedAvro() {
      // Given:
      givenAvroSchemaRegistered();

      // When:
      deserializers.format(SERIALIZED_TIME_WINDOWED_AVRO_RECORD);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItems(
          "TUMBLING(AVRO)", "HOPPING(AVRO)"
      ));
    }

    @Test
    public void shouldFormatValidTimeWindowedAvro() {
      // Given:
      givenAvroSchemaRegistered();

      // When:
      final String formatted = deserializers.format(SERIALIZED_TIME_WINDOWED_AVRO_RECORD);

      assertThat(formatted, is("[{\"str1\": \"My first string\"}@1234567890123/-]"));
    }

    @Test
    public void shouldNotExcludeSessionWindowedAvroOnValidTimeWindowedAvro() {
      // Given:
      givenAvroSchemaRegistered();

      // When:
      deserializers.format(SERIALIZED_SESSION_WINDOWED_AVRO_RECORD);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItems(
          "SESSION(AVRO)"
      ));
    }

    @Test
    public void shouldFormatValidSessionWindowedAvro() {
      // Given:
      givenAvroSchemaRegistered();

      // When:
      final String formatted = deserializers.format(SERIALIZED_SESSION_WINDOWED_AVRO_RECORD);

      // Then:
      assertThat(formatted, is("[{\"str1\": \"My first string\"}@1534567890123/1534567899999]"));
    }

    @Test
    public void shouldExcludeProtobufNoSchema() {
      // Given:
      // not: givenProtoSchemaRegistered();

      // When:
      deserializers.format(SERIALIZED_PROTOBUF_RECORD);

      // Then:
      assertThat(deserializers.getPossibleFormats(), not(hasItem("PROTOBUF")));
    }

    @Test
    public void shouldNotExcludeProtobufOnValidProtobuf() {
      // Given:
      givenProtoSchemaRegistered();

      // When:
      deserializers.format(SERIALIZED_PROTOBUF_RECORD);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItems(
          "PROTOBUF"
      ));
    }

    @Test
    public void shouldFormatValidProtobuf() {
      // Given:
      givenProtoSchemaRegistered();

      // When:
      final String formatted = deserializers.format(SERIALIZED_PROTOBUF_RECORD);

      // Then:
      assertThat(formatted, is("str1: \"My first string\" str2: \"My second string\""));
    }

    @Test
    public void shouldNotExcludeTimeWindowedProtobufOnValidTimeWindowedProtobuf() {
      // Given:
      givenProtoSchemaRegistered();

      // When:
      deserializers.format(SERIALIZED_TIME_WINDOWED_PROTOBUF_RECORD);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItems(
          "TUMBLING(PROTOBUF)", "HOPPING(PROTOBUF)"
      ));
    }

    @Test
    public void shouldFormatValidTimeWindowedProtobuf() {
      // Given:
      givenProtoSchemaRegistered();

      // When:
      final String formatted = deserializers.format(SERIALIZED_TIME_WINDOWED_PROTOBUF_RECORD);

      assertThat(formatted, is("[str1: \"My first string\" str2: \"My second string\"@1234567890123/-]"));
    }

    @Test
    public void shouldNotExcludeSessionWindowedProtobufOnValidTimeWindowedProtobuf() {
      // Given:
      givenProtoSchemaRegistered();

      // When:
      deserializers.format(SERIALIZED_SESSION_WINDOWED_PROTOBUF_RECORD);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItems(
          "SESSION(PROTOBUF)"
      ));
    }

    @Test
    public void shouldFormatValidSessionWindowedProtobuf() {
      // Given:
      givenProtoSchemaRegistered();

      // When:
      final String formatted = deserializers.format(SERIALIZED_SESSION_WINDOWED_PROTOBUF_RECORD);

      // Then:
      assertThat(formatted, is("[str1: \"My first string\" str2: \"My second string\"@1534567890123/1534567899999]"));
    }

    @Test
    public void shouldExcludeJsonSrNoSchema() {
      // Given:
      // not: givenJsonSrSchemaRegistered();

      // When:
      deserializers.format(SERIALIZED_JSON_SR_RECORD);

      // Then:
      assertThat(deserializers.getPossibleFormats(), not(hasItem("JSON_SR")));
    }

    @Test
    public void shouldNotExcludeJsonSrOnValidJsonSr() {
      // Given:
      givenJsonSrSchemaRegistered();

      // When:
      deserializers.format(SERIALIZED_JSON_SR_RECORD);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItems(
          "JSON_SR"
      ));
    }

    @Test
    public void shouldFormatValidJsonSr() {
      // Given:
      givenJsonSrSchemaRegistered();

      // When:
      final String formatted = deserializers.format(SERIALIZED_JSON_SR_RECORD);

      // Then:
      assertThat(formatted, is("{\"str1\":\"My first string\",\"str2\":\"My second string\"}"));
    }

    @Test
    public void shouldNotExcludeTimeWindowedJsonSrOnValidTimeWindowedJsonSr() {
      // Given:
      givenJsonSrSchemaRegistered();

      // When:
      deserializers.format(SERIALIZED_TIME_WINDOWED_JSON_SR_RECORD);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItems(
          "TUMBLING(JSON_SR)", "HOPPING(JSON_SR)"
      ));
    }

    @Test
    public void shouldFormatValidTimeWindowedJsonSr() {
      // Given:
      givenJsonSrSchemaRegistered();

      // When:
      final String formatted = deserializers.format(SERIALIZED_TIME_WINDOWED_JSON_SR_RECORD);

      assertThat(formatted, is("[{\"str1\":\"My first string\",\"str2\":\"My second string\"}@1234567890123/-]"));
    }

    @Test
    public void shouldNotExcludeSessionWindowedJsonSrOnValidTimeWindowedJsonSr() {
      // Given:
      givenJsonSrSchemaRegistered();

      // When:
      deserializers.format(SERIALIZED_SESSION_WINDOWED_JSON_SR_RECORD);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItems(
          "SESSION(JSON_SR)"
      ));
    }

    @Test
    public void shouldFormatValidSessionWindowedJsonSr() {
      // Given:
      givenJsonSrSchemaRegistered();

      // When:
      final String formatted = deserializers.format(SERIALIZED_SESSION_WINDOWED_JSON_SR_RECORD);

      // Then:
      assertThat(formatted, is("[{\"str1\":\"My first string\",\"str2\":\"My second string\"}@1534567890123/1534567899999]"));
    }

    @Test
    public void shouldExcludeJsonOnBadJson() {
      // Given:
      final String notJson = "{"
          + "BAD DATA"
          + "\"name\": \"myrecord\"," +
          "  \"type\": \"record\"" +
          "}";

      // When:
      deserializers.format(Bytes.wrap(notJson.getBytes(UTF_8)));

      // Then:
      assertThat(deserializers.getPossibleFormats(), not(hasItems("JSON")));
    }

    @Test
    public void shouldNotExcludeJsonOnValidJson() {
      VALID_JSON.forEach(json -> {
        // Given:
        final Bytes serialized = Bytes.wrap(json.getBytes(UTF_8));

        // When:
        deserializers.format(serialized);

        // Then:
        assertThat(json, deserializers.getPossibleFormats(), hasItems("JSON"));
      });
    }

    @Test
    public void shouldFormatValidJson() {
      VALID_JSON.forEach(json -> {
        // Given:
        final Bytes serialized = Bytes.wrap(json.getBytes(UTF_8));

        // When:
        final String formatted = deserializers.format(serialized);

        // Then:
        assertThat(formatted, is(json));
      });
    }

    @Test
    public void shouldNotExcludeTimeWindowedJsonOnTimeWindowedJson() {
      VALID_JSON.forEach(json -> {
        // Given:
        final Bytes serialized = serialize(
            new Windowed<>(json, TIME_WINDOW),
            WindowedSerdes.timeWindowedSerdeFrom(String.class, TIME_WINDOW.end() - TIME_WINDOW.start()).serializer()
        );

        // When:
        deserializers.format(serialized);

        // Then:
        assertThat(json, deserializers.getPossibleFormats(), hasItems(
            "TUMBLING(JSON)", "HOPPING(JSON)"
        ));
      });
    }

    @Test
    public void shouldFormatValidTimeWindowedJson() {
      VALID_JSON.forEach(json -> {
        // Given:
        final Bytes serialized = serialize(
            new Windowed<>(json, TIME_WINDOW),
            WindowedSerdes.timeWindowedSerdeFrom(String.class, TIME_WINDOW.end() - TIME_WINDOW.start()).serializer()
        );

        // When:
        final String formatted = deserializers.format(serialized);

        // Then:
        assertThat(formatted, is("[" + json + "@1234567890123/-]"));
      });
    }

    @Test
    public void shouldNotExcludeSessionWindowedJsonOnTimeWindowedJson() {
      VALID_JSON.forEach(json -> {
        // Given:
        final Bytes serialized = serialize(
            new Windowed<>(json, SESSION_WINDOW),
            WindowedSerdes.sessionWindowedSerdeFrom(String.class).serializer()
        );

        // When:
        deserializers.format(serialized);

        // Then:
        assertThat(json, deserializers.getPossibleFormats(), hasItems(
            "SESSION(JSON)"
        ));
      });
    }

    @Test
    public void shouldFormatValidSessionWindowedJson() {
      VALID_JSON.forEach(json -> {
        // Given:
        final Bytes serialized = serialize(
            new Windowed<>(json, SESSION_WINDOW),
            WindowedSerdes.sessionWindowedSerdeFrom(String.class).serializer()
        );

        // When:
        final String formatted = deserializers.format(serialized);

        // Then:
        assertThat(formatted, is("[" + json + "@1534567890123/1534567899999]"));
      });
    }

    @Test
    public void shouldExcludeKafkaIntIfWrongSize() {
      // When:
      deserializers.format(getBytes(SERIALIZED_INT_SIZE + 1));

      // Then:
      assertThat(deserializers.getPossibleFormats(), not(hasItem("KAFKA_INT")));
    }

    @Test
    public void shouldNotExcludeKafkaIntIfWriteSize() {
      // When:
      deserializers.format(SERIALIZED_KAFKA_INT);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItem("KAFKA_INT"));
    }

    @Test
    public void shouldFormatValidKafkaInt() {
      // When:
      final String formatted = deserializers.format(SERIALIZED_KAFKA_INT);

      // Then:
      assertThat(formatted, is(KAFKA_INT + ""));
    }

    @Test
    public void shouldNotExcludeTimeWindowedKafkaIntIfWriteSize() {
      // When:
      deserializers.format(SERIALIZED_TIME_WINDOWED_KAFKA_INT);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItems(
          "HOPPING(KAFKA_INT)", "TUMBLING(KAFKA_INT)"
      ));
    }

    @Test
    public void shouldFormatValidTimeWindowedKafkaInt() {
      // When:
      final String formatted = deserializers.format(SERIALIZED_TIME_WINDOWED_KAFKA_INT);

      // Then:
      assertThat(formatted, is("[" + KAFKA_INT + "@1234567890123/-]"));
    }

    @Test
    public void shouldNotExcludeSessionWindowedKafkaIntIfWriteSize() {
      // When:
      deserializers.format(SERIALIZED_SESSION_WINDOWED_KAFKA_INT);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItems(
          "SESSION(KAFKA_INT)"
      ));
    }

    @Test
    public void shouldFormatValidSessionWindowedKafkaInt() {
      // When:
      final String formatted = deserializers.format(SERIALIZED_SESSION_WINDOWED_KAFKA_INT);

      // Then:
      assertThat(formatted, is("[" + KAFKA_INT + "@1534567890123/1534567899999]"));
    }

    @Test
    public void shouldExcludeKafkaBigIntIfNotWriteSize() {
      // When:
      deserializers.format(getBytes(SERIALIZED_BIGINT_SIZE - 1));

      // Then:
      assertThat(deserializers.getPossibleFormats(), not(hasItem("KAFKA_BIGINT")));
    }

    @Test
    public void shouldNotExcludeKafkaBigIntIfWriteSize() {
      // When:
      deserializers.format(SERIALIZED_KAFKA_BIGINT);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItem("KAFKA_BIGINT"));
    }

    @Test
    public void shouldFormatValidKafkaBigInt() {
      // When:
      final String formatted = deserializers.format(SERIALIZED_KAFKA_BIGINT);

      // Then:
      assertThat(formatted, is(KAFKA_BIGINT + ""));
    }

    @Test
    public void shouldNotExcludeTimeWindowedKafkaBigIntIfWriteSize() {
      // When:
      deserializers.format(SERIALIZED_TIME_WINDOWED_KAFKA_BIGINT);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItems(
          "HOPPING(KAFKA_BIGINT)", "TUMBLING(KAFKA_BIGINT)"
      ));
    }

    @Test
    public void shouldFormatValidTimeWindowedKafkaBigInt() {
      // When:
      final String formatted = deserializers.format(SERIALIZED_TIME_WINDOWED_KAFKA_BIGINT);

      // Then:
      assertThat(formatted, is("[" + KAFKA_BIGINT + "@1234567890123/-]"));
    }

    @Test
    public void shouldNotExcludeSessionWindowedKafkaBigIntIfWriteSize() {
      // When:
      deserializers.format(SERIALIZED_SESSION_WINDOWED_KAFKA_BIGINT);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItems(
          "SESSION(KAFKA_BIGINT)"
      ));
    }

    @Test
    public void shouldFormatValidSessionWindowedKafkaBigInt() {
      // When:
      final String formatted = deserializers.format(SERIALIZED_SESSION_WINDOWED_KAFKA_BIGINT);

      // Then:
      assertThat(formatted, is("[" + KAFKA_BIGINT + "@1534567890123/1534567899999]"));
    }

    @Test
    public void shouldExcludeKafkaDoubleIfNotWriteSize() {
      // When:
      deserializers.format(getBytes(SERIALIZED_DOUBLE_SIZE - 1));

      // Then:
      assertThat(deserializers.getPossibleFormats(), not(hasItem("KAFKA_DOUBLE")));
    }

    @Test
    public void shouldNotExcludeKafkaDoubleIfWriteSize() {
      // When:
      deserializers.format(SERIALIZED_KAFKA_DOUBLE);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItem("KAFKA_DOUBLE"));
    }

    @Test
    public void shouldFormatValidKafkaDoubleAsBigInt() {
      // Note: Long and double are both 8 bytes and always valid.
      // Long is more populate, so ksql leans towards long

      // When:
      final String formatted = deserializers.format(SERIALIZED_KAFKA_DOUBLE);

      // Then:
      assertThat(formatted, is(longEquiv(KAFKA_DOUBLE) + ""));
    }

    @Test
    public void shouldNotExcludeTimeWindowedKafkaDoubleIfWriteSize() {
      // When:
      deserializers.format(SERIALIZED_TIME_WINDOWED_KAFKA_DOUBLE);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItems(
          "HOPPING(KAFKA_DOUBLE)", "TUMBLING(KAFKA_DOUBLE)"
      ));
    }

    @Test
    public void shouldFormatValidTimeWindowedKafkaDouble() {
      // Note: Long and double are both 8 bytes and always valid.
      // Long is more populate, so ksql leans towards long

      // When:
      final String formatted = deserializers.format(SERIALIZED_TIME_WINDOWED_KAFKA_DOUBLE);

      // Then:
      assertThat(formatted,
          is("[" + longEquiv(KAFKA_DOUBLE) + "@1234567890123/-]"));
    }

    @Test
    public void shouldNotExcludeSessionWindowedKafkaDoubleIfWriteSize() {
      // When:
      deserializers.format(SERIALIZED_SESSION_WINDOWED_KAFKA_DOUBLE);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItems(
          "SESSION(KAFKA_DOUBLE)"
      ));
    }

    @Test
    public void shouldFormatValidSessionWindowedKafkaDouble() {
      // Note: Long and double are both 8 bytes and always valid.
      // Long is more populate, so ksql leans towards long

      // When:
      final String formatted = deserializers.format(SERIALIZED_SESSION_WINDOWED_KAFKA_DOUBLE);

      // Then:
      assertThat(formatted, is("[" + longEquiv(KAFKA_DOUBLE) + "@1534567890123/1534567899999]"));
    }

    @Test
    public void shouldExcludeKafkaStringIfNotUtf8Text() {
      // When:
      deserializers.format(SERIALIZED_INVALID_UTF8);

      // Then:
      assertThat(deserializers.getPossibleFormats(), not(hasItem("KAFKA_STRING")));
    }

    @Test
    public void shouldNotExcludeKafkaStringIfValidUtf8() {
      // When:
      deserializers.format(SERIALIZED_KAFKA_STRING);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItem("KAFKA_STRING"));
    }

    @Test
    public void shouldFormatValidKafkaString() {
      // When:
      final String formatted = deserializers.format(SERIALIZED_KAFKA_STRING);

      // Then:
      assertThat(formatted, is(KAFKA_STRING));
    }

    @Test
    public void shouldNotExcludeTimeWindowedKafkaStringIfValidUtf8() {
      // When:
      deserializers.format(SERIALIZED_TIME_WINDOWED_KAFKA_STRING);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItems(
          "HOPPING(KAFKA_STRING)", "TUMBLING(KAFKA_STRING)"
      ));
    }

    @Test
    public void shouldFormatValidTimeWindowedKafkaString() {
      // When:
      final String formatted = deserializers.format(SERIALIZED_TIME_WINDOWED_KAFKA_STRING);

      // Then:
      assertThat(formatted, is("[" + KAFKA_STRING + "@1234567890123/-]"));
    }

    @Test
    public void shouldNotExcludeSessionWindowedKafkaStringIfValidUtf8() {
      // When:
      deserializers.format(SERIALIZED_SESSION_WINDOWED_KAFKA_STRING);

      // Then:
      assertThat(deserializers.getPossibleFormats(), hasItems(
          "SESSION(KAFKA_STRING)"
      ));
    }

    @Test
    public void shouldFormatValidSessionWindowedKafkaString() {
      // When:
      final String formatted = deserializers.format(SERIALIZED_SESSION_WINDOWED_KAFKA_STRING);

      // Then:
      assertThat(formatted, is("[" + KAFKA_STRING + "@1534567890123/1534567899999]"));
    }

    @Test
    public void shouldFallBackToUnrecognisedBytesIfEverythingElseFails() {
      // When:
      deserializers.format(SERIALIZED_INVALID_UTF8);

      // Then:
      assertThat(deserializers.getPossibleFormats(), is(empty()));
    }

    @Test
    public void shouldFormatUnrecognisedBytes() {
      // When:
      final String formatted = deserializers.format(SERIALIZED_INVALID_UTF8);

      // Then:
      assertThat(formatted, is(SERIALIZED_INVALID_UTF8.toString()));
    }

    @Test
    public void shouldFormatNull() {
      NULL_VARIANTS.forEach(nullVariant ->
          assertThat(deserializers.format(null), is("<null>"))
      );
    }

    private void givenAvroSchemaRegistered() {
      try {
        final AvroSchema avroSchema = new AvroSchema(AVRO_SCHEMA);
        when(schemaRegistryClient.getSchemaBySubjectAndId(any(), anyInt())).thenReturn(avroSchema);
      } catch (final Exception e) {
        fail("invalid test:" + e.getMessage());
      }
    }

    private void givenProtoSchemaRegistered() {
      try {
        when(schemaRegistryClient.getSchemaBySubjectAndId(any(), anyInt())).thenReturn(PROTOBUF_SCHEMA);
      } catch (final Exception e) {
        fail("invalid test:" + e.getMessage());
      }
    }

    private void givenJsonSrSchemaRegistered() {
      try {
        when(schemaRegistryClient.getSchemaBySubjectAndId(any(), anyInt())).thenReturn(JSON_SCHEMA);
      } catch (final Exception e) {
        fail("invalid test:" + e.getMessage());
      }
    }

    @SuppressFBWarnings(value = "DMI_RANDOM_USED_ONLY_ONCE")
    private static Bytes getBytes(final int size) {
      final byte[] bytes = new byte[size];
      RNG.nextBytes(bytes);
      return Bytes.wrap(bytes);
    }

    /**
     * No way to tell between a double and a long once serialized. KSQL defaults to long. So doubles
     * are output as longs:
     */
    @SuppressWarnings("SameParameterValue")
    private static long longEquiv(final double kafkaDouble) {
      final byte[] bytes = new DoubleSerializer().serialize("foo", kafkaDouble);
      return new LongDeserializer().deserialize("foo", bytes);
    }

    private static <T> Bytes serialize(final T value, final Serializer<T> serializer) {
      return Bytes.wrap(serializer.serialize(TOPIC_NAME, value));
    }

    @SuppressWarnings("SameParameterValue")
    private static Schema parseAvroSchema(final String avroSchema) {
      final Schema.Parser parser = new Schema.Parser();
      return parser.parse(avroSchema);
    }

    private static Record avroRecord() {
      final Record avroRecord = new Record(AVRO_SCHEMA);
      avroRecord.put("str1", "My first string");
      return avroRecord;
    }

    private static Serializer<Object> avroSerializer() {
      final Map<String, String> props = new HashMap<>();
      props.put("schema.registry.url", "localhost:9092");

      return new KafkaAvroSerializer(new MockSchemaRegistryClient(), props);
    }

    private static Message protobufRecord() {
      final org.apache.kafka.connect.data.Schema schema = SchemaBuilder.struct()
          .field("str1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
          .field("str2", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
          .build();
      return (Message) new ProtobufData().fromConnectData(
          schema,
          new Struct(schema)
              .put("str1", "My first string")
              .put("str2", "My second string")
      ).getValue();
    }

    private static Serializer<Message> protobufSerializer() {
      final Map<String, String> props = new HashMap<>();
      props.put("schema.registry.url", "localhost:9092");

      return new KafkaProtobufSerializer<>(new MockSchemaRegistryClient(), props);
    }

    private static Serializer<Object> jsonSrSerializer() {
      final Map<String, String> props = new HashMap<>();
      props.put("schema.registry.url", "localhost:9092");
      props.put("json.schema.scan.packages", "io.confluent.ksql.serde.json");

      return new KafkaJsonSchemaSerializer<>(new MockSchemaRegistryClient(), props);
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class CombinedTest {

    @Mock
    private SchemaRegistryClient schemaRegistryClient;

    private RecordFormatter formatter;

    @Before
    public void setUp() {
      formatter = new RecordFormatter(schemaRegistryClient, "some-topic");
    }

    @Test
    public void shouldReprocessBatchIfLikelyKeyFormatChanges() {
      // Given:
      final Iterable<ConsumerRecord<Bytes, Bytes>> records = consumerRecords(
          // Key that is same size as  BIGINT / DOUBLE:
          consumerRecord(Bytes.wrap("Die Hard".getBytes(UTF_8)), null),
          consumerRecord(Bytes.wrap("Key that's clearly a string".getBytes(UTF_8)), null),
          consumerRecord(Bytes.wrap("".getBytes(UTF_8)), null)
      );

      // When:
      final List<String> formatted = formatter.format(records);

      // Then:
      assertThat(formatted.get(0), containsString("Die Hard"));
      assertThat(formatted.get(1), containsString("Key that's clearly a string"));
    }

    @Test
    public void shouldReprocessBatchIfLikelyValueFormatChanges() {
      // Given:
      final Iterable<ConsumerRecord<Bytes, Bytes>> records = consumerRecords(
          // Value that is same size as  BIGINT / DOUBLE:
          consumerRecord(null, Bytes.wrap("Die Hard".getBytes(UTF_8))),
          consumerRecord(null, Bytes.wrap("Value that's clearly a string".getBytes(UTF_8))),
          consumerRecord(null, Bytes.wrap("".getBytes(UTF_8)))
      );

      // When:
      final List<String> formatted = formatter.format(records);

      // Then:
      assertThat(formatted.get(0), containsString("Die Hard"));
      assertThat(formatted.get(1), containsString("Value that's clearly a string"));
    }


    @SuppressWarnings("varargs")
    @SafeVarargs
    private static Iterable<ConsumerRecord<Bytes, Bytes>> consumerRecords(
        final ConsumerRecord<Bytes, Bytes>... records
    ) {
      return ImmutableList.copyOf(records);
    }

    private static ConsumerRecord<Bytes, Bytes> consumerRecord(
        final Bytes keyBytes,
        final Bytes valueBytes
    ) {
      return new ConsumerRecord<>(
          TOPIC_NAME,
          1,
          1,
          1234L,
          TimestampType.CREATE_TIME,
          1,
          1,
          keyBytes,
          valueBytes,
          new RecordHeaders(),
          Optional.empty()
      );
    }
  }

  @SafeVarargs
  @SuppressWarnings("varargs")
  private static <T> Matcher<Collection<T>> notHasItems(final T... items) {
    return allOf(Arrays.stream(items)
        .map(Matchers::hasItem)
        .map(Matchers::not)
        .collect(Collectors.toList()));
  }
}
