/*
 * Copyright 2020 Confluent Inc.
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.ksql.json.JsonMapper;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.SessionWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * Formats records as strings.
 *
 * <p>An instance will attempt to determine the key and value formats of the topic records it is
 * asked to process.  It does this by maintaining lists of possible formats for the keys and values
 * and removes any formats that fail to deserialize any of the data seen. In this way, the list of
 * possibilities will reduce over time.
 *
 * <p>The list of starting formats is defined in the {@link Format} enum. The list of key formats
 * also includes windowed variants, as defined in the {@link WindowSchema} enum.
 *
 * <p>Where multiple formats are still possible, the current record is formatted using the first.
 * Hence the order of formats in the list affects which format is used to format the output.
 *
 * <p>If all known formats fail the output is formatted using {@link Bytes#toString()}, which can
 * handle arbitrary bytes.
 */
public final class RecordFormatter {

  private final DateFormat dateFormat;
  private final Deserializers keyDeserializers;
  private final Deserializers valueDeserializers;

  public RecordFormatter(
      final SchemaRegistryClient schemaRegistryClient,
      final String topicName
  ) {
    this(
        SimpleDateFormat.getDateTimeInstance(3, 1, Locale.getDefault()),
        new Deserializers(topicName, schemaRegistryClient, true),
        new Deserializers(topicName, schemaRegistryClient, false)
    );
  }

  @VisibleForTesting
  RecordFormatter(
      final DateFormat dateFormat,
      final Deserializers keyDeserializers,
      final Deserializers valueDeserializers
  ) {
    this.dateFormat = requireNonNull(dateFormat, "dateFormat");
    this.keyDeserializers = requireNonNull(keyDeserializers, "keyDeserializers");
    this.valueDeserializers = requireNonNull(valueDeserializers, "valueDeserializers");
  }

  public List<Supplier<String>> format(final Iterable<ConsumerRecord<Bytes, Bytes>> records) {
    return StreamSupport.stream(records.spliterator(), false)
        .map(this::delayedFormat)
        .collect(Collectors.toList());
  }

  /**
   * Returns the list of key formats that are capable of deserializing all the keys seen without
   * error.
   *
   * <p>As more records are passed to {@link #format} the list is refined.
   *
   * @return the list of compatible key formats
   */
  public List<String> getPossibleKeyFormats() {
    return keyDeserializers.getPossibleFormats();
  }

  /**
   * Returns the list of value formats that are capable of deserializing all the values seen without
   * error.
   *
   * <p>As more records are passed to {@link #format} the list is refined.
   *
   * @return the list of compatible value formats
   */
  public List<String> getPossibleValueFormats() {
    return valueDeserializers.getPossibleFormats();
  }

  private Supplier<String> delayedFormat(final ConsumerRecord<Bytes, Bytes> record) {
    return () -> "rowtime: " + formatRowTime(record.timestamp())
        + ", " + "key: " + keyDeserializers.format(record.key())
        + ", value: " + valueDeserializers.format(record.value());
  }

  private String formatRowTime(final long timestamp) {
    return timestamp == ConsumerRecord.NO_TIMESTAMP
        ? "N/A"
        : dateFormat.format(new Date(timestamp));
  }

  private static Deserializer<?> newJsonDeserializer() {
    final String replacement = UTF_8.newDecoder().replacement();

    return (Deserializer<Object>) (topic, data) -> {
      if (data.length == 0) {
        throw new DeserializationException("Empty data");
      }

      final String text = new String(data, UTF_8);
      if (text.contains(replacement)) {
        throw new DeserializationException("String contains replacement char");
      }

      try {
        // test it parses:
        JsonMapper.INSTANCE.mapper.readTree(text);

        // but return actual text:
        return text;
      } catch (final IOException e) {
        throw new DeserializationException("Failed to deserialize as JSON", e);
      }
    };
  }

  private static Deserializer<?> newStringDeserializer() {
    final StringDeserializer deserializer = new StringDeserializer();
    final String replacement = UTF_8.newDecoder().replacement();

    return (Deserializer<Object>) (topic, data) -> {
      if (data.length == 0) {
        throw new DeserializationException("Empty data");
      }

      final String text = deserializer.deserialize("", data);
      if (text.contains(replacement)) {
        throw new DeserializationException("String contains replacement char");
      }

      return text;
    };
  }

  @VisibleForTesting
  static final class Deserializers {

    private final String topicName;
    private final List<NamedDeserializer> deserializers;

    @SuppressWarnings("UnstableApiUsage")
    Deserializers(
        final String topicName,
        final SchemaRegistryClient schemaRegistryClient,
        final boolean incWindowed
    ) {
      this.topicName = requireNonNull(topicName, "topicName");

      final List<NamedDeserializer> deserializers = Arrays.stream(Format.values())
          .map(format -> format.getDeserializer(schemaRegistryClient))
          .collect(Collectors.toList());

      if (!incWindowed) {
        this.deserializers = deserializers;
      } else {
        this.deserializers = deserializers.stream()
            .flatMap(deserializer ->
                deserializer.doNotWrap
                    ? Stream.of(deserializer)
                    : Streams.concat(
                        Arrays.stream(WindowSchema.values()).map(ws -> ws.wrap(deserializer)),
                        Stream.of(deserializer)
                    ))
            .collect(Collectors.toList());
      }
    }

    List<String> getPossibleFormats() {
      return deserializers.stream()
          .map(NamedDeserializer::toString)
          .filter(name -> !name.equals(Format.UNRECOGNISED_BYTES.toString()))
          .collect(Collectors.toList());
    }

    String format(final Bytes bytes) {
      if (bytes == null || bytes.get() == null) {
        return "<null>";
      }

      String firstResult = null;
      final Iterator<NamedDeserializer> it = deserializers.iterator();
      while (it.hasNext()) {
        final Optional<String> possibleResult = tryDeserializer(bytes, it.next());
        if (possibleResult.isPresent() && firstResult == null) {
          firstResult = possibleResult.get();
        }

        if (!possibleResult.isPresent()) {
          it.remove();
        }
      }

      return firstResult == null
          ? "<Failed to deserialize>"
          : firstResult;
    }

    private Optional<String> tryDeserializer(
        final Bytes bytes,
        final NamedDeserializer deserializer
    ) {
      try {
        final Object result = deserializer.deserializer.deserialize(topicName, bytes.get());
        return Optional.of(result == null ? "<null>" : deserializer.format(result));
      } catch (final Exception e) {
        return Optional.empty();
      }
    }
  }

  enum WindowSchema {

    SESSION(WindowSchema::newSessionWindowedDeserializer),
    HOPPING(WindowSchema::newTimeWindowedDeserializer),
    TUMBLING(WindowSchema::newTimeWindowedDeserializer);

    private final Function<NamedDeserializer, Deserializer<?>> mapper;

    WindowSchema(final Function<NamedDeserializer, Deserializer<?>> mapper) {
      this.mapper = requireNonNull(mapper, "mapper");
    }

    public NamedDeserializer wrap(final NamedDeserializer inner) {

      final String name = name() + "(" + inner.name + ")";

      final Deserializer<?> deserializer = mapper.apply(inner);

      return new NamedDeserializer(name, inner.doNotWrap, deserializer, inner::format);
    }

    private static Deserializer<?> newSessionWindowedDeserializer(
        final NamedDeserializer inner
    ) {
      final SessionWindowedDeserializer<?> sessionDeser
          = new SessionWindowedDeserializer<>(inner.deserializer);

      return (topic, data) -> {
        final Windowed<?> windowed = sessionDeser.deserialize(topic, data);
        return "[" + inner.format(windowed.key())
            + "@" + windowed.window().start() + "/" + windowed.window().end() + "]";
      };
    }

    private static Deserializer<?> newTimeWindowedDeserializer(
        final NamedDeserializer inner
    ) {
      final TimeWindowedDeserializer<?> windowedDeser
          = new TimeWindowedDeserializer<>(inner.deserializer);

      return (topic, data) -> {
        final Windowed<?> windowed = windowedDeser.deserialize(topic, data);

        // Exclude window end time for time-windowed as the end time is not in the serialized data:
        return "[" + inner.format(windowed.key()) + "@" + windowed.window().start() + "/-]";
      };
    }
  }

  enum Format {
    AVRO(0, KafkaAvroDeserializer::new, Object::toString),
    PROTOBUF(
        0,
        KafkaProtobufDeserializer::new,
            o -> TextFormat.printer().shortDebugString((Message) o)),
    JSON(RecordFormatter::newJsonDeserializer),
    KAFKA_INT(IntegerDeserializer::new),
    KAFKA_BIGINT(LongDeserializer::new),
    KAFKA_DOUBLE(DoubleDeserializer::new),
    KAFKA_STRING(RecordFormatter::newStringDeserializer),
    UNRECOGNISED_BYTES(BytesDeserializer::new);

    private final Function<SchemaRegistryClient, Deserializer<?>> deserializerFactory;
    private Function<Object, String> formatObject;

    Format(final Supplier<Deserializer<?>> deserializerFactory) {
      this(1, srClient -> deserializerFactory.get(), Object::toString);
    }

    @SuppressWarnings("unused")
    Format(
        final int usedOnlyToDifferentiateWhichConstructorIsCalled,
        final Function<SchemaRegistryClient, Deserializer<?>> deserializerFactory,
        final Function<Object, String> formatObject
    ) {
      this.deserializerFactory = requireNonNull(deserializerFactory, "deserializerFactory");
      this.formatObject = requireNonNull(formatObject, "formatObject");
    }

    NamedDeserializer getDeserializer(final SchemaRegistryClient srClient) {
      final Deserializer<?> deserializer = deserializerFactory.apply(srClient);
      return new NamedDeserializer(name(), this == UNRECOGNISED_BYTES, deserializer, formatObject);
    }
  }

  private static final class NamedDeserializer {

    final String name;
    final boolean doNotWrap;
    final Deserializer<?> deserializer;
    private final Function<Object, String> formatObj;

    private NamedDeserializer(
        final String name,
        final boolean doNotWrap,
        final Deserializer<?> deserializer,
        final Function<Object, String> formatObj
    ) {
      this.name = requireNonNull(name, "name");
      this.doNotWrap = doNotWrap;
      this.deserializer = requireNonNull(deserializer, "deserializer");
      this.formatObj = requireNonNull(formatObj, "formatObj");
    }

    @Override
    public String toString() {
      return name;
    }

    public String format(final Object o) {
      if (o instanceof String) {
        return (String) o;
      }
      return formatObj.apply(o);
    }
  }

  private static final class DeserializationException extends RuntimeException {

    DeserializationException(final String msg) {
      super(msg);
    }

    DeserializationException(final String msg, final Throwable cause) {
      super(msg, cause);
    }
  }
}
