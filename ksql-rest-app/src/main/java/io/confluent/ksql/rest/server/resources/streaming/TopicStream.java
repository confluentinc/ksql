/*
 * Copyright 2018 Confluent Inc.
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
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.ksql.rest.util.JsonMapper;
import io.confluent.ksql.util.SchemaUtil;
import java.io.IOException;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TopicStream {

  private TopicStream() {
  }

  public static class RecordFormatter {

    private static final Logger log = LoggerFactory.getLogger(RecordFormatter.class);

    private final KafkaAvroDeserializer avroDeserializer;
    private final String topicName;
    private final DateFormat dateFormat =
        SimpleDateFormat.getDateTimeInstance(3, 1, Locale.getDefault());

    private Formatter formatter;

    public RecordFormatter(final SchemaRegistryClient schemaRegistryClient,
                           final String topicName) {
      this.topicName = Objects.requireNonNull(topicName, "topicName");
      this.avroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
    }

    public List<String> format(final ConsumerRecords<String, Bytes> records) {
      return StreamSupport
          .stream(records.records(topicName).spliterator(), false)
          .filter(Objects::nonNull)
          .filter(r -> r.value() != null)
          .map((record) -> {
            if (formatter == null) {
              formatter = getFormatter(record);
            }
            try {
              return formatter.print(record);
            } catch (IOException e) {
              log.warn("Exception formatting record", e);
              return null;
            }
          })
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
    }

    public Format getFormat() {
      return formatter == null ? Format.UNDEFINED : formatter.getFormat();
    }

    private Formatter getFormatter(final ConsumerRecord<String, Bytes> record) {
      return Arrays.stream(Format.values())
          .map(f -> f.maybeGetFormatter(topicName, record, avroDeserializer, dateFormat))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .findFirst()
          .orElseThrow(() -> new RuntimeException("Unexpected"));
    }
  }

  interface Formatter {

    String print(ConsumerRecord<String, Bytes> consumerRecord) throws IOException;

    Format getFormat();
  }

  enum Format {
    UNDEFINED {
    },
    AVRO {
      @Override
      public Optional<Formatter> maybeGetFormatter(
          final String topicName,
          final ConsumerRecord<String, Bytes> record,
          final KafkaAvroDeserializer avroDeserializer,
          final DateFormat dateFormat) {
        try {
          avroDeserializer.deserialize(topicName, record.value().get());
          return Optional.of(createFormatter(topicName, avroDeserializer, dateFormat));
        } catch (final Throwable t) {
          return Optional.empty();
        }
      }

      private Formatter createFormatter(final String topicName,
                                        final KafkaAvroDeserializer avroDeserializer,
                                        final DateFormat dateFormat) {
        return new Formatter() {
          @Override
          public String print(final ConsumerRecord<String, Bytes> consumerRecord) {
            final String time = dateFormat.format(new Date(consumerRecord.timestamp()));

            final GenericRecord record = (GenericRecord) avroDeserializer.deserialize(
                topicName, consumerRecord.value().get());

            final String key = consumerRecord.key() != null ? consumerRecord.key() : "null";
            return time + ", " + key + ", " + record.toString() + "\n";
          }

          @Override
          public Format getFormat() {
            return AVRO;
          }
        };
      }
    },
    JSON {
      @Override
      public Optional<Formatter> maybeGetFormatter(
          final String topicName,
          final ConsumerRecord<String, Bytes> record,
          final KafkaAvroDeserializer avroDeserializer,
          final DateFormat dateFormat) {
        try {
          JsonMapper.INSTANCE.mapper.readTree(record.value().toString());

          return Optional.of(createFormatter());
        } catch (final Throwable t) {
          return Optional.empty();
        }
      }

      private Formatter createFormatter() {
        return new Formatter() {
          @Override
          public String print(final ConsumerRecord<String, Bytes> record)
              throws IOException {

            final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;
            final JsonNode jsonNode = objectMapper.readTree(record.value().toString());
            final ObjectNode objectNode = objectMapper.createObjectNode();
            final String key = (record.key() != null) ? record.key() : "null";

            objectNode.put(SchemaUtil.ROWTIME_NAME, record.timestamp());
            objectNode.put(SchemaUtil.ROWKEY_NAME, key);
            objectNode.setAll((ObjectNode) jsonNode);

            final StringWriter stringWriter = new StringWriter();
            objectMapper.writeValue(stringWriter, objectNode);
            return stringWriter.toString() + "\n";
          }

          @Override
          public Format getFormat() {
            return JSON;
          }
        };
      }
    },
    STRING {
      @Override
      public Optional<Formatter> maybeGetFormatter(
          final String topicName,
          final ConsumerRecord<String, Bytes> record,
          final KafkaAvroDeserializer avroDeserializer,
          final DateFormat dateFormat) {
        // STRING always returns a formatter because its last in the enum list
        return Optional.of(createFormatter(dateFormat));
      }

      private Formatter createFormatter(final DateFormat dateFormat) {
        return new Formatter() {

          @Override
          public String print(final ConsumerRecord<String, Bytes> record) {
            final String key = record.key() != null ? record.key() : "NULL";
            final String value = record.value() != null ? record.value().toString() : "NULL";
            return dateFormat.format(new Date(record.timestamp())) + " , " + key
                   + " , " + value + "\n";
          }

          @Override
          public Format getFormat() {
            return STRING;
          }
        };
      }
    };

    Optional<Formatter> maybeGetFormatter(
        final String topicName,
        final ConsumerRecord<String, Bytes> record,
        final KafkaAvroDeserializer avroDeserializer,
        final DateFormat dateFormat) {
      return Optional.empty();
    }
  }
}
