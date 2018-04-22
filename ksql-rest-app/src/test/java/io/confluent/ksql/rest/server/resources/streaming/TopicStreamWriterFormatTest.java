package io.confluent.ksql.rest.server.resources.streaming;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TopicStreamWriterFormatTest {

  @Test
  public void shouldMatchAvroFormatter() throws Exception {

    /**
     * Build an AVRO message
     */
    String USER_SCHEMA = "{\n" +
            "    \"fields\": [\n" +
            "        { \"name\": \"str1\", \"type\": \"string\" }\n" +
            "    ],\n" +
            "    \"name\": \"myrecord\",\n" +
            "    \"type\": \"record\"\n" +
            "}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(USER_SCHEMA);

    GenericData.Record avroRecord = new GenericData.Record(schema);
    avroRecord.put("str1", "My first string");

    /**
     * Setup expects
     */
    SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    expect(schemaRegistryClient.register(anyString(), anyObject())).andReturn(1);
    expect(schemaRegistryClient.getById(anyInt())).andReturn(schema);

    replay(schemaRegistryClient);


    Map<String, String> props = new HashMap<>();
    props.put("schema.registry.url", "localhost:9092");

    KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(schemaRegistryClient, props);


    /**
     * Test data
     */
    byte[] testRecordBytes = avroSerializer.serialize("topic", avroRecord);
    ConsumerRecord<String, Bytes> record = new ConsumerRecord<String, Bytes>("topic", 1, 1, "key", new Bytes(testRecordBytes));

    /** Assert
     */
    assertTrue(TopicStreamWriter.Format.AVRO.isFormat("topic", record, schemaRegistryClient));
  }

  @Test
  public void shouldNotMatchAvroFormatter() throws Exception {

    /**
     * Setup expects
     */
    SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    replay(schemaRegistryClient);

    /**
     * Test data
     */
    ConsumerRecord<String, Bytes> record = new ConsumerRecord<String, Bytes>("topic", 1, 1, "key", new Bytes("test-data".getBytes()));

    /** Assert
     */
    assertFalse(TopicStreamWriter.Format.AVRO.isFormat("topic", record, schemaRegistryClient));
  }

  @Test
  public void shouldMatchJsonFormatter() throws Exception {

    SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    replay(schemaRegistryClient);

    /**
     * Test data
     */
    String json = "{    \"name\": \"myrecord\"," +
            "    \"type\": \"record\"" +
            "}";

    ConsumerRecord<String, Bytes> record = new ConsumerRecord<String, Bytes>("topic", 1, 1, "key", new Bytes(json.getBytes()));

    assertTrue(TopicStreamWriter.Format.JSON.isFormat("topic", record, schemaRegistryClient));
  }

  @Test
  public void shouldNotMatchJsonFormatter() throws Exception {

    SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    replay(schemaRegistryClient);

    /**
     * Test data
     */
    String json = "{  BAD DATA  \"name\": \"myrecord\"," +
            "    \"type\": \"record\"" +
            "}";

    ConsumerRecord<String, Bytes> record = new ConsumerRecord<String, Bytes>("topic", 1, 1, "key", new Bytes(json.getBytes()));


    assertFalse(TopicStreamWriter.Format.JSON.isFormat("topic", record, schemaRegistryClient));
  }
}