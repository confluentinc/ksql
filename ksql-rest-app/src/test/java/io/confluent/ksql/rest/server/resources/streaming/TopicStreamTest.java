package io.confluent.ksql.rest.server.resources.streaming;

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.ksql.rest.server.resources.streaming.TopicStream.Format;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.junit.Test;

public class TopicStreamTest {

  @Test
  public void shouldMatchAvroFormatter() throws Exception {

    /**
     * Build an AVRO message
     */
    final String USER_SCHEMA = "{\n" +
            "    \"fields\": [\n" +
            "        { \"name\": \"str1\", \"type\": \"string\" }\n" +
            "    ],\n" +
            "    \"name\": \"myrecord\",\n" +
            "    \"type\": \"record\"\n" +
            "}";
    final Schema.Parser parser = new Schema.Parser();
    final Schema schema = parser.parse(USER_SCHEMA);

    final GenericData.Record avroRecord = new GenericData.Record(schema);
    avroRecord.put("str1", "My first string");

    /**
     * Setup expects
     */
    final SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    expect(schemaRegistryClient.register(anyString(), anyObject())).andReturn(1);
    expect(schemaRegistryClient.getById(anyInt())).andReturn(schema);

    replay(schemaRegistryClient);


    final Map<String, String> props = new HashMap<>();
    props.put("schema.registry.url", "localhost:9092");

    final KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(schemaRegistryClient, props);


    /**
     * Test data
     */
    final byte[] testRecordBytes = avroSerializer.serialize("topic", avroRecord);
    final ConsumerRecord<String, Bytes> record = new ConsumerRecord<String, Bytes>("topic", 1, 1, "key", new Bytes(testRecordBytes));

    /** Assert
     */
    assertTrue(Format.AVRO.isFormat("topic", record, schemaRegistryClient));
  }

  @Test
  public void shouldNotMatchAvroFormatter() throws Exception {

    /**
     * Setup expects
     */
    final SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    replay(schemaRegistryClient);

    /**
     * Test data
     */
    final ConsumerRecord<String, Bytes> record = new ConsumerRecord<String, Bytes>("topic", 1, 1, "key", new Bytes("test-data".getBytes()));

    /** Assert
     */
    assertFalse(Format.AVRO.isFormat("topic", record, schemaRegistryClient));
  }

  @Test
  public void shouldMatchJsonFormatter() throws Exception {

    final SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    replay(schemaRegistryClient);

    /**
     * Test data
     */
    final String json = "{    \"name\": \"myrecord\"," +
            "    \"type\": \"record\"" +
            "}";

    final ConsumerRecord<String, Bytes> record = new ConsumerRecord<String, Bytes>("topic", 1, 1, "key", new Bytes(json.getBytes()));

    assertTrue(Format.JSON.isFormat("topic", record, schemaRegistryClient));
  }

  @Test
  public void shouldNotMatchJsonFormatter() throws Exception {

    final SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    replay(schemaRegistryClient);

    /**
     * Test data
     */
    final String json = "{  BAD DATA  \"name\": \"myrecord\"," +
            "    \"type\": \"record\"" +
            "}";

    final ConsumerRecord<String, Bytes> record = new ConsumerRecord<String, Bytes>("topic", 1, 1, "key", new Bytes(json.getBytes()));
    assertFalse(Format.JSON.isFormat("topic", record, schemaRegistryClient));
  }

  @Test
  public void shouldHandleNullValues() throws Exception {
    final SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    replay(schemaRegistryClient);

    final ConsumerRecord<String, Bytes> record
        = new ConsumerRecord<>("topic", 1, 1, "key", null);

    final String[] printedData = Format.STRING.print(record).split(",");
    assertEquals(3, printedData.length);
    assertEquals("key", printedData[1].trim());
    assertEquals("NULL", printedData[2].trim());

  }
}
