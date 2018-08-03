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
import org.junit.Test;

public class TopicStreamTest {

  private SchemaRegistryClient schemaRegistryClient;

  @Before
  public void setUp() {
    schemaRegistryClient = mock(SchemaRegistryClient.class);
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

    expect(schemaRegistryClient.register(anyString(), anyObject())).andReturn(1);
    expect(schemaRegistryClient.getById(anyInt())).andReturn(schema).times(2);

    replay(schemaRegistryClient);

    final byte[] avroData = serializeAvroRecord(avroRecord);

    // When:
    final Result result = getFormatter(avroData);

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
    final Result result = getFormatter(notAvro);

    // Then:
    assertThat(result.format, is(not(Format.AVRO)));
  }

  @Test
  public void shouldFormatJson() {
    // Given:
    replay(schemaRegistryClient);

    final String json =
        "{    \"name\": \"myrecord\"," +
        "    \"type\": \"record\"" +
        "}";

    // When:
    final Result result = getFormatter(json);

    // Then:
    assertThat(result.format, is(Format.JSON));
    assertThat(result.formatted,
               is("{\"ROWTIME\":-1,\"ROWKEY\":\"key\",\"name\":\"myrecord\",\"type\":\"record\"}\n"));
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
    final Result result = getFormatter(notJson);

    // Then:
    assertThat(result.format, is(not(Format.JSON)));
  }

  @Test
  public void shouldFilterNullValues() {
    replay(schemaRegistryClient);

    final ConsumerRecord<String, Bytes> record = new ConsumerRecord<>(
        "some-topic", 1, 1, "key", null);
    final RecordFormatter formatter =
        new RecordFormatter(schemaRegistryClient, "some-topic");
    final ConsumerRecords<String, Bytes> records = new ConsumerRecords<>(
        ImmutableMap.of(new TopicPartition("some-topic", 1),
            ImmutableList.of(record)));

    assertThat(formatter.format(records), empty());
  }

  @Test
  public void shouldHandleNullValuesFromSTRINGPrint() throws IOException {
    final DateFormat dateFormat =
        SimpleDateFormat.getDateTimeInstance(3, 1, Locale.getDefault());

    final ConsumerRecord<String, Bytes> record = new ConsumerRecord<>(
        "some-topic", 1, 1, "key", null);

    final String formatted =
        Format.STRING.maybeGetFormatter(
            "some-topic", record, null, dateFormat).get().print(record);

    assertThat(formatted, endsWith(", key , NULL\n"));
  }

  private Result getFormatter(final String data) {
    return getFormatter(data.getBytes(StandardCharsets.UTF_8));
  }

  private Result getFormatter(final byte[] data) {
    final ConsumerRecord<String, Bytes> record = new ConsumerRecord<>(
        "some-topic", 1, 1, "key", new Bytes(data));

    final RecordFormatter formatter =
        new RecordFormatter(schemaRegistryClient, "some-topic");

    final ConsumerRecords<String, Bytes> records = new ConsumerRecords<>(
        ImmutableMap.of(new TopicPartition("some-topic", 1),
                        ImmutableList.of(record)));

    final List<String> formatted = formatter.format(records);
    assertThat("Only expect one line", formatted, hasSize(1));

    return new Result(formatter.getFormat(), formatted.get(0));
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
