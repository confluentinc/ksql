package io.confluent.ksql.util;


import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.serde.JsonPOJODeserializer;
import io.confluent.ksql.serde.JsonPOJOSerializer;
import jline.console.ConsoleReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class TopicPrinter {

    static Serde<GenericRow> genericRowSerde = null;

    public void printGenericRowTopic(String topicName, ConsoleReader console) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, topicName+"_"+System.currentTimeMillis());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, GenericRow> source = builder.stream(Serdes.String(), getGenericRowSerde(), topicName);

        source.map(new KeyValueMapper<String, GenericRow, KeyValue<String,GenericRow>>() {
            @Override
            public KeyValue<String, GenericRow> apply(String key, GenericRow row) {
                try {
                    console.println(row.toString());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return new KeyValue<String, GenericRow>(key, row);
            }
        });

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        streams.close();
    }

    private static Serde<GenericRow> getGenericRowSerde() {
        if(genericRowSerde == null) {
            Map<String, Object> serdeProps = new HashMap<>();

            final Serializer<GenericRow> genericRowSerializer = new JsonPOJOSerializer<>();
            serdeProps.put("JsonPOJOClass", GenericRow.class);
            genericRowSerializer.configure(serdeProps, false);

            final Deserializer<GenericRow> genericRowDeserializer = new JsonPOJODeserializer<>();
            serdeProps.put("JsonPOJOClass", GenericRow.class);
            genericRowDeserializer.configure(serdeProps, false);

            genericRowSerde = Serdes.serdeFrom(genericRowSerializer,  genericRowDeserializer);
        }
        return genericRowSerde;
    }

}
