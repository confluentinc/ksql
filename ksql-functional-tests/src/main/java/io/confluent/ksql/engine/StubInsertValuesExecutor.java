/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.test.serde.string.StringSerdeSupplier;
import io.confluent.ksql.test.tools.Record;
import io.confluent.ksql.test.tools.Topic;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.test.tools.stubs.StubKafkaRecord;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class StubInsertValuesExecutor {

  private StubInsertValuesExecutor() {
  }

  public static InsertValuesExecutor of(final StubKafkaService stubKafkaService) {
    final StubProducer stubProducer = new StubProducer(stubKafkaService);

    return new InsertValuesExecutor(
        false,
        (record, ignored1, ingnored2) -> stubProducer.sendRecord(record));
  }

  @VisibleForTesting
  static class StubProducer {

    private final StubKafkaService stubKafkaService;

    StubProducer(final StubKafkaService stubKafkaService) {
      this.stubKafkaService = Objects.requireNonNull(stubKafkaService, "stubKafkaService");
    }

    void sendRecord(final ProducerRecord<byte[], byte[]> record) {
      final Object value = getValue(record);

      this.stubKafkaService.writeRecord(record.topic(),
          StubKafkaRecord.of(
              new Record(
                  stubKafkaService.getTopic(record.topic()),
                  new String(record.key(), StandardCharsets.UTF_8),
                  value,
                  Optional.of(record.timestamp()),
                  null
              ),
              null)
      );
    }

    private Object getValue(final ProducerRecord<byte[], byte[]> record) {
      final Topic topic = stubKafkaService.getTopic(record.topic());

      final Object value;
      if (topic.getValueSerdeSupplier() instanceof StringSerdeSupplier) {
        value = new String(record.value(), StandardCharsets.UTF_8);
      } else {
        try {
          value = new ObjectMapper().readValue(record.value(), Object.class);
        } catch (final IOException e) {
          throw new InvalidFieldException("value", "failed to parse", e);
        }
      }
      return value;
    }
  }
}
