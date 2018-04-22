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

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.ksql.util.SchemaUtil;

import static io.confluent.ksql.rest.server.resources.streaming.TopicStreamWriter.Format.getFormatter;

public class TopicStreamWriter implements StreamingOutput {

  public enum Format {

    UNDEFINED {
    },
    AVRO {
      private String topicName;
      private KafkaAvroDeserializer avroDeserializer;

      @Override
      public boolean isFormat(
          String topicName, ConsumerRecord<String, Bytes> record,
          SchemaRegistryClient schemaRegistryClient
      ) {
        this.topicName = topicName;
        try {
          avroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
          avroDeserializer.deserialize(topicName, record.value().get());
          return true;
        } catch (Throwable t) {
          return false;
        }
      }

      @Override
      String print(ConsumerRecord<String, Bytes> consumerRecord) {
        String time = dateFormat.format(new Date(consumerRecord.timestamp()));
        GenericRecord record = (GenericRecord) avroDeserializer.deserialize(
            topicName,
            consumerRecord
                .value()
                .get()
        );
        String key = consumerRecord.key() != null ? consumerRecord.key() : "null";
        return time + ", " + key + ", " + record.toString() + "\n";
      }
    },
    JSON {
      final ObjectMapper objectMapper = new ObjectMapper();

      @Override
      public boolean isFormat(
          String topicName, ConsumerRecord<String, Bytes> record,
          SchemaRegistryClient schemaRegistryClient
      ) {
        try {
          objectMapper.readTree(record.value().toString());
          return true;
        } catch (Throwable t) {
          return false;
        }
      }

      @Override
      String print(ConsumerRecord<String, Bytes> record) throws IOException {
        JsonNode jsonNode = objectMapper.readTree(record.value().toString());
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put(SchemaUtil.ROWTIME_NAME, record.timestamp());
        objectNode.put(SchemaUtil.ROWKEY_NAME, (record.key() != null) ? record.key() : "null");
        objectNode.setAll((ObjectNode) jsonNode);
        StringWriter stringWriter = new StringWriter();
        objectMapper.writeValue(stringWriter, objectNode);
        return stringWriter.toString() + "\n";
      }
    },
    STRING {
      @Override
      public boolean isFormat(
          String topicName,
          ConsumerRecord<String, Bytes> record,
          SchemaRegistryClient schemaRegistryClient
      ) {
        /**
         * STRING always returns true because its last in the enum list
         */
        return true;
      }
    };

    final DateFormat dateFormat = SimpleDateFormat.getDateTimeInstance(3, 1, Locale.getDefault());

    static Format getFormatter(
        String topicName,
        ConsumerRecord<String, Bytes> record,
        SchemaRegistryClient schemaRegistryClient
    ) {
      Format result = Format.UNDEFINED;
      while (!(result.isFormat(topicName, record, schemaRegistryClient))) {
        result = Format.values()[result.ordinal() + 1];
      }
      return result;

    }

    boolean isFormat(
        String topicName,
        ConsumerRecord<String, Bytes> record,
        SchemaRegistryClient schemaRegistryClient
    ) {
      return false;
    }

    String print(ConsumerRecord<String, Bytes> record) throws IOException {
      String key = record.key() != null ? record.key() : "null";
      return dateFormat.format(new Date(record.timestamp())) + " , " + key
          + " , " + record.value().toString() + "\n";
    }

  }

  private static final Logger log = LoggerFactory.getLogger(TopicStreamWriter.class);
  private final Long interval;
  private final long disconnectCheckInterval;
  private final KafkaConsumer<String, Bytes> topicConsumer;
  private final SchemaRegistryClient schemaRegistryClient;
  private final String topicName;

  private long messagesWritten;

  public TopicStreamWriter(
      SchemaRegistryClient schemaRegistryClient,
      Map<String, Object> consumerProperties,
      String topicName,
      long interval,
      long disconnectCheckInterval,
      boolean fromBeginning
  ) {
    this.schemaRegistryClient = schemaRegistryClient;
    this.topicName = topicName;
    this.messagesWritten = 0;

    this.disconnectCheckInterval = disconnectCheckInterval;

    this.topicConsumer = new KafkaConsumer<>(
        consumerProperties,
        new StringDeserializer(),
        new BytesDeserializer()
    );

    List<TopicPartition> topicPartitions = topicConsumer.partitionsFor(topicName)
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
      Format format = Format.UNDEFINED;
      while (true) {
        ConsumerRecords<String, Bytes> records = topicConsumer.poll(disconnectCheckInterval);
        if (records.isEmpty()) {
          out.write("\n".getBytes(StandardCharsets.UTF_8));
          out.flush();
        } else {
          for (ConsumerRecord<String, Bytes> record : records.records(topicName)) {
            if (record.value() != null) {
              if (format == Format.UNDEFINED) {
                format = getFormatter(topicName, record, schemaRegistryClient);
                out.write(("Format:" + format.name() + "\n").getBytes(StandardCharsets.UTF_8));
              }
              if (messagesWritten++ % interval == 0) {
                out.write(format.print(record).getBytes(StandardCharsets.UTF_8));
                out.flush();
              }
            }
          }
        }
      }
    } catch (EOFException exception) {
      // Connection terminated, we can stop writing
    } catch (Exception exception) {
      log.error("Exception encountered while writing to output stream", exception);
      out.write(exception.getMessage().getBytes(StandardCharsets.UTF_8));
      out.write("\n".getBytes(StandardCharsets.UTF_8));
      out.flush();
    } finally {
      topicConsumer.close();
    }
  }
}
