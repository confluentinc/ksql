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

package io.confluent.ksql.rest.server.resources.streaming;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.ksql.rest.server.resources.streaming.TopicStream.Format;
import io.confluent.ksql.rest.server.resources.streaming.TopicStream.RecordFormatter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings({"UnstableApiUsage", "unchecked"})
@RunWith(MockitoJUnitRunner.class)
public class TopicStreamTest {

  private static final byte[] NULL = "null-marker".getBytes(UTF_8);

  private static final String TOPIC_NAME = "some-topic";

  private static final String STRING_KEY = "key";
  private static final String JSON_OBJECT = "{\"a\":1}";
  private static final String JSON_ARRAY = "[10,22,44]";
  private static final Schema AVRO_SCHEMA = parseAvroSchema("{" +
      "    \"type\": \"record\"," +
      "    \"name\": \"myrecord\"," +
      "    \"fields\": [" +
      "        { \"name\": \"str1\", \"type\": \"string\" }" +
      "    ]" +
      "}");
  private static final byte[] SERIALIZED_AVRO_RECORD = serializedAvroRecord();
  private static final String DELIMITED_VALUE = "De,lim,it,ed";
  private static final byte[] RANDOM_BYTES = new byte[]{23, 45, 63, 23, 1, 0, 1, 99, 101};
  private static final byte[][] NO_ADDITIONAL_ITEMS = new byte[0][];
  private static final List<Bytes> NULL_VARIANTS;

  private static final DateFormat UTC_FORMAT = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss +0000");

  static {
    final List<Bytes> nullVariants = new ArrayList<>();
    nullVariants.add(new Bytes(null));
    nullVariants.add(null);
    NULL_VARIANTS = Collections.unmodifiableList(nullVariants);
  }

  @Mock
  private SchemaRegistryClient schemaRegistryClient;

  private RecordFormatter formatter;
  private long timestamp = 1581366404000L;

  @Before
  public void setUp() throws Exception {
    formatter = new RecordFormatter(schemaRegistryClient, TOPIC_NAME, UTC_FORMAT);

    when(schemaRegistryClient.getSchemaById(anyInt()))
        .thenReturn(new AvroSchema(AVRO_SCHEMA));
  }

  @Test
  public void shouldDetectAvroKey() {
    // When:
    formatSingle(SERIALIZED_AVRO_RECORD, NULL);

    // Then:
    assertThat(formatter.getKeyFormat(), is(Format.AVRO));
  }

  @Test
  public void shouldDetectJsonObjectKey() {
    // When:
    formatSingle(JSON_OBJECT.getBytes(UTF_8), SERIALIZED_AVRO_RECORD);

    // Then:
    assertThat(formatter.getKeyFormat(), is(Format.JSON));
  }

  @Test
  public void shouldDetectJsonObjectValue() {
    // When:
    formatSingle(SERIALIZED_AVRO_RECORD, JSON_OBJECT.getBytes(UTF_8));

    // Then:
    assertThat(formatter.getValueFormat(), is(Format.JSON));
  }

  @Test
  public void shouldDetectJsonArrayKey() {
    // When:
    formatSingle(JSON_ARRAY.getBytes(UTF_8), NULL);

    // Then:
    assertThat(formatter.getKeyFormat(), is(Format.JSON));
  }

  @Test
  public void shouldDetectJsonArrayValue() {
    // When:
    formatSingle(NULL, JSON_ARRAY.getBytes(UTF_8));

    // Then:
    assertThat(formatter.getValueFormat(), is(Format.JSON));
  }

  @Test
  public void shouldDetectBadJsonAsString() {
    // Given:
    final String notJson = "{"
        + "BAD DATA"
        + "\"name\": \"myrecord\"," +
        "  \"type\": \"record\"" +
        "}";

    // When:
    formatSingle(SERIALIZED_AVRO_RECORD, notJson.getBytes(UTF_8));

    // Then:
    assertThat(formatter.getValueFormat(), is(Format.STRING));
  }

  @Test
  public void shouldDetectDelimitedAsStringKey() {
    // When:
    formatSingle(DELIMITED_VALUE.getBytes(UTF_8), NULL);

    // Then:
    assertThat(formatter.getKeyFormat(), is(Format.STRING));
  }

  @Test
  public void shouldDetectDelimitedAsStringValue() {
    // When:
    formatSingle(NULL, DELIMITED_VALUE.getBytes(UTF_8));

    // Then:
    assertThat(formatter.getValueFormat(), is(Format.STRING));
  }

  @Test
  public void shouldDetectRandomBytesAsStringKey() {
    // When:
    formatSingle(RANDOM_BYTES, NULL);

    // Then:
    assertThat(formatter.getKeyFormat(), is(Format.STRING));
  }

  @Test
  public void shouldDetectRandomBytesAsStringValue() {
    // When:
    formatSingle(NULL, RANDOM_BYTES);

    // Then:
    assertThat(formatter.getValueFormat(), is(Format.STRING));
  }

  @Test
  public void shouldDetectMixedModeKey() {
    // When:
    formatKeys(
        JSON_OBJECT.getBytes(UTF_8),
        DELIMITED_VALUE.getBytes(UTF_8),
        SERIALIZED_AVRO_RECORD
    );

    // Then:
    assertThat(formatter.getKeyFormat(), is(Format.MIXED));
  }

  @Test
  public void shouldDetectMixedModeValue() {
    // When:
    formatValues(
        JSON_OBJECT.getBytes(UTF_8),
        DELIMITED_VALUE.getBytes(UTF_8),
        SERIALIZED_AVRO_RECORD
    );

    // Then:
    assertThat(formatter.getValueFormat(), is(Format.MIXED));
  }

  @Test
  public void shouldDeferFormatDetectionOnNulls() {
    // When:
    format(NULL_VARIANTS, NULL_VARIANTS);

    // Then:
    assertThat(formatter.getKeyFormat(), is(Format.UNDEFINED));
    assertThat(formatter.getValueFormat(), is(Format.UNDEFINED));
  }

  @Test
  public void shouldDetermineKeyFormatsOnSecondCallIfNoViableRecordsInFirst() {
    // Given:
    formatSingle(NULL, NULL);

    // When:
    formatKeys(NULL, JSON_OBJECT.getBytes(UTF_8), NULL);

    // Then:
    assertThat(formatter.getKeyFormat(), is(Format.JSON));
  }

  @Test
  public void shouldDetermineValueFormatsOnSecondCallIfNoViableRecordsInFirst() {
    // Given:
    formatSingle(NULL, NULL);

    // When:
    formatValues(NULL, JSON_OBJECT.getBytes(UTF_8), NULL);

    // Then:
    assertThat(formatter.getValueFormat(), is(Format.JSON));
  }

  @Test
  public void shouldOutputRowTime() {
    // When:
    final String formatted = formatSingle(NULL, NULL);

    // Then:
    assertThat(formatted, containsString("rowtime: 02/10/2020 20:26:44 +0000, "));
  }

  @Test
  public void shouldOutputRowTimeAsNaIfNa() {
    // Given:
    timestamp = ConsumerRecord.NO_TIMESTAMP;

    // When:
    final String formatted = formatSingle(NULL, NULL);

    // Then:
    assertThat(formatted, containsString("rowtime: N/A, "));
  }

  @Test
  public void shouldFormatAvroKey() {
    // When:
    final String formatted = formatSingle(SERIALIZED_AVRO_RECORD, NULL);

    // Then:
    assertThat(formatted, containsString(", key: {\"str1\": \"My first string\"}, "));
  }

  @Test
  public void shouldFormatAvroValue() {
    // When:
    final String formatted = formatSingle(NULL, SERIALIZED_AVRO_RECORD);

    // Then:
    assertThat(formatted, endsWith(", value: {\"str1\": \"My first string\"}"));
  }

  @Test
  public void shouldFormatJsonObjectKey() {
    // When:
    final String formatted = formatSingle(JSON_OBJECT.getBytes(UTF_8), NULL);

    // Then:
    assertThat(formatted, containsString(", key: " + JSON_OBJECT + ", "));
  }

  @Test
  public void shouldFormatJsonObjectValue() {
    // When:
    final String formatted = formatSingle(NULL, JSON_OBJECT.getBytes(UTF_8));

    // Then:
    assertThat(formatted, containsString(", value: " + JSON_OBJECT));
  }

  @Test
  public void shouldFormatJsonArrayKey() {
    // When:
    final String formatted = formatSingle(JSON_ARRAY.getBytes(UTF_8), NULL);

    // Then:
    assertThat(formatted, containsString(", key: " + JSON_ARRAY + ", "));
  }

  @Test
  public void shouldFormatJsonArrayValue() {
    // When:
    final String formatted = formatSingle(NULL, JSON_ARRAY.getBytes(UTF_8));

    // Then:
    assertThat(formatted, containsString(", value: " + JSON_ARRAY));
  }

  @Test
  public void shouldFormatDelimitedAsStringKey() {
    // When:
    final String formatted = formatSingle(DELIMITED_VALUE.getBytes(UTF_8), NULL);

    // Then:
    assertThat(formatted, containsString(", key: " + DELIMITED_VALUE + ", "));
  }

  @Test
  public void shouldFormatDelimitedAsStringValue() {
    // When:
    final String formatted = formatSingle(NULL, DELIMITED_VALUE.getBytes(UTF_8));

    // Then:
    assertThat(formatted, containsString(", value: " + DELIMITED_VALUE));
  }

  @Test
  public void shouldFormatRandomBytesAsStringKey() {
    // When:
    final String formatted = formatSingle(RANDOM_BYTES, NULL);

    // Then:
    assertThat(formatted, containsString(", key: " + Bytes.wrap(RANDOM_BYTES).toString() + ", "));
  }

  @Test
  public void shouldFormatRandomBytesAsStringValue() {
    // When:
    final String formatted = formatSingle(NULL, RANDOM_BYTES);

    // Then:
    assertThat(formatted, containsString(", value: " + Bytes.wrap(RANDOM_BYTES).toString()));
  }

  @Test
  public void shouldDefaultToStringFormattingInMixedMode() {
    // When:
    final List<String> results = formatValues(
        JSON_OBJECT.getBytes(UTF_8),
        DELIMITED_VALUE.getBytes(UTF_8),
        SERIALIZED_AVRO_RECORD
    );

    // Then:
    assertThat(results, contains(
        containsString(", value: " + JSON_OBJECT),
        containsString(", value: " + DELIMITED_VALUE),
        containsString(", value: " + Bytes.wrap(SERIALIZED_AVRO_RECORD).toString())
    ));
  }

  @Test
  public void shouldFormatNulls() {
    // When:
    final List<String> formatted = format(NULL_VARIANTS, NULL_VARIANTS);

    // Then:
    assertThat(formatted, contains(
        containsString(", value: <null>"),
        containsString(", value: <null>")
    ));
  }

  @Test
  public void shouldFormatNullJsonRecord() {
    // Given:
    formatSingle(NULL, JSON_OBJECT.getBytes(UTF_8));

    // When:
    final String formatted = formatSingle(NULL, "null".getBytes(UTF_8));

    // Then:
    assertThat(formatted, containsString(", value: null"));
  }

  private String formatSingle(final byte[] key, final byte[] value) {
    final List<String> formatted = format(
        Collections.singletonList(toBytes(key)),
        Collections.singletonList(toBytes(value))
    );
    assertThat("Only expect one line", formatted, hasSize(1));

    return formatted.get(0);
  }

  private List<String> formatKeys(final byte[] first, final byte[]... others) {

    final List<Bytes> keys = Streams
        .concat(Stream.of(first), Arrays.stream(others))
        .map(TopicStreamTest::toBytes)
        .collect(Collectors.toList());

    final List<Bytes> values = IntStream.range(0, keys.size())
        .mapToObj(idx -> (Bytes)null)
        .collect(Collectors.toList());

    return format(keys, values);
  }

 private List<String> formatValues(final byte[] first, final byte[]... others) {

    final List<Bytes> values = Streams
        .concat(Stream.of(first), Arrays.stream(others))
        .map(TopicStreamTest::toBytes)
        .collect(Collectors.toList());

    final List<Bytes> keys = IntStream.range(0, values.size())
        .mapToObj(idx -> (Bytes)null)
        .collect(Collectors.toList());

    return format(keys, values);
  }

  private List<String> format(final List<Bytes> keys, final List<Bytes> values) {
    assertThat("invalid test", keys, hasSize(values.size()));

    final List<ConsumerRecord<Bytes, Bytes>> recs = IntStream.range(0, keys.size())
        .mapToObj(idx -> new ConsumerRecord<>(TOPIC_NAME, 1, 1, timestamp,
            TimestampType.CREATE_TIME, 123, 1, 1,
            keys.get(idx), values.get(idx)))
        .collect(Collectors.toList());

    final ConsumerRecords<Bytes, Bytes> records =
        new ConsumerRecords<>(ImmutableMap.of(new TopicPartition(TOPIC_NAME, 1), recs));

    return formatter.format(records).stream()
        .map(Supplier::get)
        .collect(Collectors.toList());
  }

  private static Bytes toBytes(final byte[] key) {
    return key == NULL ? null : Bytes.wrap(key);
  }

  @SuppressWarnings("SameParameterValue")
  private static Schema parseAvroSchema(final String avroSchema) {
    final Schema.Parser parser = new Schema.Parser();
    return parser.parse(avroSchema);
  }

  private static byte[] serializedAvroRecord() {
    final GenericData.Record avroRecord = new GenericData.Record(AVRO_SCHEMA);
    avroRecord.put("str1", "My first string");

    final Map<String, String> props = new HashMap<>();
    props.put("schema.registry.url", "localhost:9092");

    final SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    final KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(schemaRegistryClient, props);

    return avroSerializer.serialize("topic", avroRecord);
  }
}

// Todo(ac): Could output _possible_ formats, e.g. "23" could be JSON number, or delimited.
// Todo(ac): over multiple lines, it could narrow this down.