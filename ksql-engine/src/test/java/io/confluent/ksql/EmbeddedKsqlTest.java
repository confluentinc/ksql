package io.confluent.ksql;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.serialization.Serdes.String;
import static org.junit.Assert.assertEquals;

public class EmbeddedKsqlTest {
    private static final String ORDERS = "orders";
    private String key = "Order1";
    private IntegrationTestHarness harness;
    private KsqlContext ksqlContext;

    @Before
    public void before() throws Exception {
        harness = new IntegrationTestHarness();
        harness.start();
        ksqlContext = new KsqlContext(harness.ksqlConfig.getKsqlStreamConfigProps());
        harness.createTopic(ORDERS);
    }

    @After
    public void after() throws Exception {
        ksqlContext.close();
        harness.stop();
    }

    private static String schemaStr = "{"
            + "\"namespace\": \"confluent.io.ksql\","
            + " \"name\": \"Orders\","
            + " \"type\": \"record\","
            + " \"fields\": ["
            + "     {\"name\": \"orderId\",  \"type\": \"string\"},"
            + "     {\"name\": \"orderTime\", \"type\": \"long\"},"
            + "     {\"name\": \"description\", \"type\": \"string\"},"
            + "     {\"name\": \"units\", \"type\": \"int\"}"
            + " ]"
            + "}";

    @Test
    public void shouldRunEmbeddedKsqlQueryWithAvroEncodings() throws Exception {
        Schema schema = new Schema.Parser().parse(schemaStr);

        //Given an order, encoded in Avro, and written to the orders topic
        GenericRecord order = createOrderObject(schema, key, "foo1234", 100);
        harness.produceRecord(ORDERS, key, order, String().serializer(), avroSerializer(schema));

        //And a context with slightly hacky injected properties
        Map<String, Object> overriddenProperties = new HashMap<>();
        overriddenProperties.put("AVROSCHEMA", schemaStr);
        KsqlContext ksqlContext = ksqlContext(overriddenProperties);

        //When we run a query that pushes messages into a different topic
        ksqlContext.sql("CREATE STREAM orders (ORDERID string, ORDERTIME bigint, DESCRIPTION varchar, UNITS bigint) " +
                "WITH (kafka_topic='orders', value_format='AVRO');");
        ksqlContext.sql("CREATE STREAM bigorders AS SELECT * FROM orders WHERE UNITS > 40;");

        //Then we should see an avro encoded message in the new topic
        assertEquals(order,
                harness.consumeMessages("BIGORDERS", 1, String().deserializer(), avroDeserializer(schema)).get(key));
    }

    private KsqlContext ksqlContext(Map<String, Object> overriddenProperties) {
        Map<String, Object> streamsProperties = new HashMap<>();

        streamsProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, harness.embeddedKafkaCluster.bootstrapServers());
        streamsProperties.put("application.id", "KSQL");
        streamsProperties.put("commit.interval.ms", 0);
        streamsProperties.put("cache.max.bytes.buffering", 0);
        streamsProperties.put("auto.offset.reset", "earliest");

        return new KsqlContext(streamsProperties, overriddenProperties);
    }

    private GenericRecord createOrderObject(Schema schema, String id, String desc, int units) {
        GenericRecord order = new GenericData.Record(schema);
        order.put("orderId", id);
        order.put("orderTime", System.currentTimeMillis());
        order.put("description", desc);
        order.put("units", units);
        return order;
    }

    private Serializer<GenericRecord> avroSerializer(final Schema schema) {
        return new Serializer<GenericRecord>() {
            private DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);

            @Override
            public void configure(Map<String, ?> map, boolean b) {
            }

            @Override
            public byte[] serialize(String topic, GenericRecord value) {
                try {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                    writer.write(value, encoder);
                    encoder.flush();
                    out.close();
                    return out.toByteArray();
                } catch (IOException e) {
                    e.printStackTrace();
                    return new byte[]{};
                }
            }

            @Override
            public void close() {

            }
        };
    }

    private Deserializer<GenericRecord> avroDeserializer(final Schema schema) {
        return new Deserializer<GenericRecord>() {
            private DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

            @Override
            public void configure(Map<String, ?> map, boolean b) {
            }

            @Override
            public GenericRecord deserialize(String s, byte[] bytes) {
                Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
                try {
                    return datumReader.read(null, decoder);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public void close() {

            }
        };
    }
}