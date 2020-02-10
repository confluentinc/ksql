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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.ksql.json.JsonMapper;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
    private final DateFormat dateFormat;

    private Optional<Formatter> keyFormatter = Optional.empty();
    private Optional<Formatter> valueFormatter = Optional.empty();

    public RecordFormatter(
        final SchemaRegistryClient schemaRegistryClient,
        final String topicName
    ) {
      this(
          schemaRegistryClient,
          topicName,
          SimpleDateFormat.getDateTimeInstance(3, 1, Locale.getDefault())
      );
    }

    @VisibleForTesting
    RecordFormatter(
        final SchemaRegistryClient schemaRegistryClient,
        final String topicName,
        final DateFormat dateFormat
    ) {
      this.topicName = requireNonNull(topicName, "topicName");
      this.avroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
      this.dateFormat = requireNonNull(dateFormat, "dateFormat");
    }

    public List<Supplier<String>> format(final Iterable<ConsumerRecord<Bytes, Bytes>> records) {
      if (!keyFormatter.isPresent()) {
        keyFormatter = getKeyFormatter(records);
      }

      if (!valueFormatter.isPresent()) {
        valueFormatter = getValueFormatter(records);
      }

      return StreamSupport.stream(records.spliterator(), false)
          .map(this::delayedFormat)
          .collect(Collectors.toList());
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent") // will not be empty if needed
    private Supplier<String> delayedFormat(final ConsumerRecord<Bytes, Bytes> record) {
      return () -> {
        try {
          final String rowTime = record.timestamp() == ConsumerRecord.NO_TIMESTAMP
              ? "N/A"
              : dateFormat.format(new Date(record.timestamp()));

          final String rowKey = record.key() == null || record.key().get() == null
              ? "<null>"
              : keyFormatter.get().print(record.key());

          final String value = record.value() == null || record.value().get() == null
              ? "<null>"
              : valueFormatter.get().print(record.value());

          return "rowtime: " + rowTime
              + ", " + "key: " + rowKey
              + ", value: " + value;
        } catch (IOException e) {
          // Todo(ac): If this throws... try other formats? Output format at start of row?
          log.warn("Exception formatting record", e);
          return "Failed to parse row";
        }
      };
    }

    // Todo(ac): Detect windwowed key?
    // Todo(ac): Look in SR

    public Format getKeyFormat() {
      return keyFormatter
          .map(Formatter::getFormat)
          .orElse(Format.UNDEFINED);
    }

    public Format getValueFormat() {
      return valueFormatter
          .map(Formatter::getFormat)
          .orElse(Format.UNDEFINED);
    }

    private Optional<Formatter> getKeyFormatter(
        final Iterable<ConsumerRecord<Bytes, Bytes>> records
    ) {
      if (Iterables.isEmpty(records)) {
        return Optional.empty();
      }

      final Stream<Bytes> valueStream = StreamSupport
          .stream(records.spliterator(), false)
          .map(ConsumerRecord::key);

      return findFormatter(valueStream);
    }

    private Optional<Formatter> getValueFormatter(
        final Iterable<ConsumerRecord<Bytes, Bytes>> records
    ) {
      if (Iterables.isEmpty(records)) {
        return Optional.empty();
      }

      final Stream<Bytes> valueStream = StreamSupport
          .stream(records.spliterator(), false)
          .map(ConsumerRecord::value);

      return findFormatter(valueStream);
    }

    private Optional<Formatter> findFormatter(final Stream<Bytes> dataStream) {
      final List<Formatter> formatters = dataStream
          .filter(Objects::nonNull)
          .filter(d -> d.get() != null)
          .map(this::findFormatter)
          .collect(Collectors.toList());

      final Set<Format> formats = formatters.stream()
          .map(Formatter::getFormat)
          .collect(Collectors.toSet());

      switch (formats.size()) {
        case 0:
          // No viable records (will try again with next batch):
          return Optional.empty();

        case 1:
          // Single format:
          return Optional.of(formatters.get(0));

        default:
          // Mixed format topic:
          return Format.MIXED.maybeGetFormatter(topicName, null, avroDeserializer);
      }
    }

    private Formatter findFormatter(final Bytes data) {
      return Arrays.stream(Format.values())
          .map(f -> f.maybeGetFormatter(topicName, data, avroDeserializer))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .findFirst()
          .orElseThrow(() -> new IllegalStateException("Unexpected"));
    }
  }

  interface Formatter {

    String print(Bytes data) throws IOException;

    Format getFormat();
  }

  enum Format {
    UNDEFINED {
      @Override
      public Optional<Formatter> maybeGetFormatter(
          final String topicName,
          final Bytes data,
          final KafkaAvroDeserializer avroDeserializer
      ) {
        return Optional.empty();
      }
    },
    AVRO {
      @Override
      public Optional<Formatter> maybeGetFormatter(
          final String topicName,
          final Bytes data,
          final KafkaAvroDeserializer avroDeserializer
      ) {
        try {
          // Todo(ac): Include details that schema is registered?
          avroDeserializer.deserialize(topicName, data.get());
          return Optional.of(createFormatter(topicName, avroDeserializer));
        } catch (final Exception t) {
          return Optional.empty();
        }
      }

      private Formatter createFormatter(
          final String topicName,
          final KafkaAvroDeserializer avroDeserializer
      ) {
        return new Formatter() {
          @Override
          public String print(final Bytes data) {
            return avroDeserializer.deserialize(topicName, data.get())
                .toString();
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
          final Bytes data,
          final KafkaAvroDeserializer avroDeserializer
      ) {
        try {
          final JsonNode jsonNode = JsonMapper.INSTANCE.mapper.readTree(data.toString());

          if (!(jsonNode instanceof ObjectNode) && !(jsonNode instanceof ArrayNode)) {
            // Other valid JSON types, e.g. NumericNode, BooleanNode, etc
            // are indistinguishable from single column delimited format:
            return Optional.empty();
          }

          return Optional.of(createFormatter());
        } catch (final Exception t) {
          return Optional.empty();
        }
      }

      private Formatter createFormatter() {
        return new Formatter() {
          @Override
          public String print(final Bytes data) throws IOException {
            // Ensure deserializes to validate JSON:
            JsonMapper.INSTANCE.mapper.readTree(data.get());

            // Return data as string:
            return data.toString();
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
          final Bytes data,
          final KafkaAvroDeserializer avroDeserializer
      ) {
        // STRING always returns a formatter:
        return Optional.of(createStringFormatter(STRING));
      }
    },
    MIXED {
      @Override
      public Optional<Formatter> maybeGetFormatter(
          final String topicName,
          final Bytes data,
          final KafkaAvroDeserializer avroDeserializer
      ) {
        // Mixed mode defaults to string values:
        return Optional.of(createStringFormatter(MIXED));
      }
    };

    abstract Optional<Formatter> maybeGetFormatter(
        String topicName,
        Bytes data,
        KafkaAvroDeserializer avroDeserializer
    );

    private static Formatter createStringFormatter(final Format format) {
      return new Formatter() {

        @Override
        public String print(final Bytes data) {
          return data.toString();
        }

        @Override
        public Format getFormat() {
          return format;
        }
      };
    }
  }
}
