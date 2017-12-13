/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.server.resources.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.serde.avro.KsqlGenericRowAvroDeserializer;
import io.confluent.ksql.util.SchemaUtil;

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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class TopicStreamWriter implements StreamingOutput {

  private static final Logger log = LoggerFactory.getLogger(TopicStreamWriter.class);
  private final Long interval;
  private final long disconnectCheckInterval;
  private final KafkaConsumer<?, ?> topicConsumer;
  private final String kafkaTopic;
  KsqlTopic ksqlTopic;
  private final ObjectMapper objectMapper;

  private long messagesWritten;

  public TopicStreamWriter(
      Map<String, Object> consumerProperties,
      KsqlTopic ksqlTopic,
      long interval,
      long disconnectCheckInterval,
      boolean fromBeginning
  ) {
    this.ksqlTopic = ksqlTopic;
    this.kafkaTopic = ksqlTopic.getKafkaTopicName();
    this.messagesWritten = 0;
    this.objectMapper = new ObjectMapper();

    Deserializer<?> valueDeserializer;
    switch (ksqlTopic.getKsqlTopicSerDe().getSerDe()) {
      case JSON:
      case DELIMITED:
        valueDeserializer = new StringDeserializer();
        break;
      case AVRO:
        KsqlAvroTopicSerDe avroTopicSerDe = (KsqlAvroTopicSerDe) ksqlTopic.getKsqlTopicSerDe();
        valueDeserializer = new KsqlGenericRowAvroDeserializer(null, KsqlEngine.getSchemaRegistryClient(), false);
        break;
      default:
        throw new RuntimeException(String.format(
            "Unexpected SerDe type: %s",
            ksqlTopic.getDataSourceType().name()
        ));
    }

    this.disconnectCheckInterval = disconnectCheckInterval;

    this.topicConsumer =
        new KafkaConsumer<>(consumerProperties, new StringDeserializer(), valueDeserializer);
    List<TopicPartition> topicPartitions = topicConsumer.partitionsFor(kafkaTopic)
        .stream()
        .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
        .collect(Collectors.toList());
    topicConsumer.assign(topicPartitions);

    if (fromBeginning) {
      topicConsumer.seekToBeginning(topicPartitions);
    }

    this.interval = interval;
  }

  @Override
  public void write(OutputStream out) throws IOException, WebApplicationException {
    try {
      while (true) {
        ConsumerRecords<?, ?> records = topicConsumer.poll(disconnectCheckInterval);
        if (records.isEmpty()) {
          synchronized (out) {
            out.write("\n".getBytes(StandardCharsets.UTF_8));
            out.flush();
          }
        } else {
          synchronized (out) {
            for (ConsumerRecord<?, ?> record : records.records(kafkaTopic)) {
              if (record.value() != null) {
                if (messagesWritten++ % interval == 0) {
                  if (ksqlTopic.getKsqlTopicSerDe().getSerDe() == DataSource.DataSourceSerDe.JSON) {
                    printJsonValue(out, record);
                  } else {
                    printAvroOrDelimitedValue(out, record);
                  }
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
        out.write(exception.getMessage().getBytes(StandardCharsets.UTF_8));
        out.write("\n".getBytes(StandardCharsets.UTF_8));
        out.flush();
      }
    } finally {
      topicConsumer.close();
    }
  }

  private void printJsonValue(OutputStream out, ConsumerRecord<?, ?> record) throws IOException {
    JsonNode jsonNode = objectMapper.readTree(record.value().toString());
    ObjectNode objectNode = objectMapper.createObjectNode();
    objectNode.put(SchemaUtil.ROWTIME_NAME, record.timestamp());
    objectNode.put(SchemaUtil.ROWKEY_NAME, (record.key() != null)? record.key()
        .toString(): "null");
    objectNode.setAll((ObjectNode) jsonNode);
    objectMapper.writeValue(out, objectNode);
    out.write("\n".getBytes(StandardCharsets.UTF_8));
    out.flush();
  }

  private void printAvroOrDelimitedValue(OutputStream out, ConsumerRecord<?, ?> record) throws
                                                                                   IOException {
    out.write((record.timestamp() + " , " +record.key().toString() + " , " + record.value()
        .toString()).getBytes(StandardCharsets.UTF_8));
    out.write("\n".getBytes(StandardCharsets.UTF_8));
    out.flush();
  }

}
