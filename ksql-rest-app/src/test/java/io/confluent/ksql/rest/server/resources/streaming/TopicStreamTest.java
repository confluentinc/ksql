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

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.ksql.rest.server.resources.streaming.TopicStream.Format;
import io.confluent.ksql.rest.server.resources.streaming.TopicStream.RecordFormatter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TopicStreamTest {

  private static final String TOPIC_NAME = "some-topic";

  private SchemaRegistryClient schemaRegistryClient;
  private RecordFormatter formatter;

  @Before
  public void setUp() {
    schemaRegistryClient = mock(SchemaRegistryClient.class);
    formatter = new RecordFormatter(schemaRegistryClient, TOPIC_NAME);
  }

  @Test
  public void shouldMatchAvroFormatter() throws Exception {
    // Given:
    final Schema schema = parseAvroSchema(
        "{\n" +
        "    \"fields\": [\n" +
        "        { \"name\": \"str1\", \"type\": \"string\" }\n" +
        "    ],\n" +
        "    \"name\": \"myrecord\",\n" +
        "    \"type\": \"record\"\n" +
        "}");

    final GenericData.Record avroRecord = new GenericData.Record(schema);
    avroRecord.put("str1", "My first string");

    expect(schemaRegistryClient.register(anyString(), anyObject(ParsedSchema.class))).andReturn(1);
    expect(schemaRegistryClient.getSchemaById(anyInt())).andReturn(new AvroSchema(schema)).times(2);

    replay(schemaRegistryClient);

    final byte[] avroData = serializeAvroRecord(avroRecord);

    // When:
    final Result result = getFormattedResult(avroData);

    // Then:
    assertThat(result.format, is(Format.AVRO));
    assertThat(result.formatted, endsWith(", key, {\"str1\": \"My first string\"}\n"));
  }

  @Test
  public void shouldNotMatchAvroFormatter() {
    // Given:
    replay(schemaRegistryClient);
    final String notAvro = "test-data";

    // When:
    final Result result = getFormattedResult(notAvro);

    // Then:
    assertThat(result.format, is(not(Format.AVRO)));
  }

  @Test
  public void shouldFormatJson() {
    // Given:
    replay(schemaRegistryClient);

    final String json =
        "{    \"name\": \"myrecord\"," +
        "    \"aDecimal\": 1.1234512345123451234" +
        "}";

    // When:
    final Result result = getFormattedResult(json);

    // Then:
    assertThat(result.format, is(Format.JSON));
    assertThat(result.formatted,
               is("{\"ROWTIME\":-1,\"ROWKEY\":\"key\",\"name\":\"myrecord\",\"aDecimal\":1.1234512345123451234}\n"));
  }

  @Test
  public void shouldNotMatchJsonFormatter() {
    // Given:
    replay(schemaRegistryClient);

    final String notJson =
        "{  BAD DATA  \"name\": \"myrecord\"," +
        "    \"type\": \"record\"" +
        "}";

    // When:
    final Result result = getFormattedResult(notJson);

    // Then:
    assertThat(result.format, is(not(Format.JSON)));
  }

  @Test
  public void shouldMatchStringFormatWithOneColumnValues() {
    // Given:
    replay(schemaRegistryClient);

    final String stringValue = "v1";

    // When:
    final Result result = getFormattedResult(stringValue);

    // Then:
    assertThat(result.format, is(Format.STRING));
  }

  @Test
  public void shouldFilterNullValues() {
    // Given:
    replay(schemaRegistryClient);

    // When:
    final List<String> formatted = getFormattedRecord(null);

    // Then:
    assertThat(formatted, empty());
  }

  @Test
  public void shouldFilterNullBytesValues() {
    // Given:
    replay(schemaRegistryClient);

    // When:
    final List<String> formatted = getFormattedRecord(new Bytes(null));

    // Then:
    assertThat(formatted, empty());
  }

  @Test
  public void shouldFilterEmptyValues() {
    // Given:
    replay(schemaRegistryClient);

    // When:
    final List<String> formatted = getFormattedRecord(new Bytes(Bytes.EMPTY));

    // Then:
    assertThat(formatted, empty());
  }

  @Test
  public void shouldHandleNullValuesFromSTRINGPrint() throws IOException {
    final DateFormat dateFormat =
        SimpleDateFormat.getDateTimeInstance(3, 1, Locale.getDefault());

    final ConsumerRecord<String, Bytes> record = new ConsumerRecord<>(
        TOPIC_NAME, 1, 1, "key", null);

    final String formatted =
        Format.STRING.maybeGetFormatter(
            TOPIC_NAME, record, null, dateFormat).get().print(record);

    assertThat(formatted, endsWith(", key , NULL\n"));
  }

  private Result getFormattedResult(final String data) {
    return getFormattedResult(data.getBytes(StandardCharsets.UTF_8));
  }

  private Result getFormattedResult(final byte[] data) {
    final List<String> formatted = getFormattedRecord(new Bytes(data));
    assertThat("Only expect one line", formatted, hasSize(1));

    return new Result(formatter.getFormat(), formatted.get(0));
  }

  private List<String> getFormattedRecord(final Bytes data) {
    final ConsumerRecord<String, Bytes> record = new ConsumerRecord<>(
        TOPIC_NAME, 1, 1, "key", data);

    final ConsumerRecords<String, Bytes> records = new ConsumerRecords<>(
        ImmutableMap.of(new TopicPartition(TOPIC_NAME, 1),
            ImmutableList.of(record)));

    return formatter.format(records);
  }

  @SuppressWarnings("SameParameterValue")
  private static Schema parseAvroSchema(final String avroSchema) {
    final Schema.Parser parser = new Schema.Parser();
    return parser.parse(avroSchema);
  }

  private byte[] serializeAvroRecord(final GenericData.Record avroRecord) {
    final Map<String, String> props = new HashMap<>();
    props.put("schema.registry.url", "localhost:9092");

    final KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(schemaRegistryClient, props);

    return avroSerializer.serialize("topic", avroRecord);
  }

  private static final class Result {

    private final Format format;
    private final String formatted;

    private Result(final Format format, final String formatted) {
      this.format = format;
      this.formatted = formatted;
    }
  }
}
