/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.server.resources.streaming;


import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.serde.avro.KsqlGenericRowAvroDeserializer;
import io.confluent.ksql.serde.avro.KsqlGenericRowAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class TopicStreamWriter implements StreamingOutput {

  private static final Logger log = LoggerFactory.getLogger(TopicStreamWriter.class);
  private final Long interval;
  private final long disconnectCheckInterval;
  private final KafkaConsumer<?, ?> topicConsumer;
  private final String kafkaTopic;
  private final ObjectMapper objectMapper;

  private long messagesWritten;

  public TopicStreamWriter(
      Map<String, Object> consumerProperties,
      KsqlTopic ksqlTopic,
      long interval,
      long disconnectCheckInterval
  ) {
    this.kafkaTopic = ksqlTopic.getKafkaTopicName();
    this.messagesWritten = 0;
    this.objectMapper = new ObjectMapper();

    Deserializer<?> valueDeserializer;
    switch (ksqlTopic.getKsqlTopicSerDe().getSerDe()) {
      case JSON:
      case CSV:
        valueDeserializer = new StringDeserializer();
        break;
      case AVRO:
        KsqlAvroTopicSerDe avroTopicSerDe = (KsqlAvroTopicSerDe) ksqlTopic.getKsqlTopicSerDe();
        Map<String, Object> avroSerdeProps = new HashMap<>();
        avroSerdeProps.put(KsqlGenericRowAvroSerializer.AVRO_SERDE_SCHEMA_CONFIG, avroTopicSerDe.getSchemaString());
        valueDeserializer = new KsqlGenericRowAvroDeserializer(null);
        valueDeserializer.configure(avroSerdeProps, false);
        break;
      default:
        throw new RuntimeException(String.format("Unexpected SerDe type: %s", ksqlTopic.getDataSourceType().name()));
    }

    this.disconnectCheckInterval = disconnectCheckInterval;

    this.topicConsumer = new KafkaConsumer<>(consumerProperties, new StringDeserializer(), valueDeserializer);
    List<TopicPartition> topicPartitions = topicConsumer.partitionsFor(kafkaTopic)
        .stream()
        .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
        .collect(Collectors.toList());
    topicConsumer.assign(topicPartitions);

    this.interval = interval;
  }

  @Override
  public void write(OutputStream out) throws IOException, WebApplicationException {
    try {
      while (true) {
        ConsumerRecords<?, ?> records = topicConsumer.poll(disconnectCheckInterval);
        if (records.isEmpty()) {
          synchronized (out) {
            out.write("\n".getBytes());
            out.flush();
          }
        } else {
          synchronized (out) {
            for (ConsumerRecord<?, ?> record : records.records(kafkaTopic)) {
              if (record.value() != null) {
                if (messagesWritten++ % interval == 0) {
                  objectMapper.writeValue(out, record.value());
                  out.write("\n".getBytes());
                  out.flush();
                }
              }
            }
          }
        }
      }
    } catch (EOFException exception) {
      // Connection terminated, we can stop writing
    } catch (Exception exception) {
      log.error("Exception encountered while writing to output stream", exception);
      synchronized (out) {
        out.write(exception.getMessage().getBytes());
        out.write("\n".getBytes());
        out.flush();
      }
    } finally {
      topicConsumer.close();
    }
  }
}
