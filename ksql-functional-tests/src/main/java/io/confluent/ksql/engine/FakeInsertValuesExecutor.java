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
import io.confluent.ksql.rest.server.execution.InsertValuesExecutor;
import io.confluent.ksql.test.serde.string.StringSerdeSupplier;
import io.confluent.ksql.test.tools.FakeKafkaRecord;
import io.confluent.ksql.test.tools.FakeKafkaService;
import io.confluent.ksql.test.tools.Record;
import io.confluent.ksql.test.tools.Topic;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class FakeInsertValuesExecutor  {

  private FakeInsertValuesExecutor() {
  }

  public static InsertValuesExecutor of(final FakeKafkaService fakeKafkaService) {
    final FakeProduer fakeProduer = new FakeProduer(fakeKafkaService);

    return new InsertValuesExecutor(
        false,
        (record, ignored1, ingnored2) -> fakeProduer.sendRecord(record));
  }

  @VisibleForTesting
  static class FakeProduer {

    private final FakeKafkaService fakeKafkaService;

    FakeProduer(final FakeKafkaService fakeKafkaService) {
      this.fakeKafkaService = Objects.requireNonNull(fakeKafkaService, "fakeKafkaService");
    }

    void sendRecord(final ProducerRecord<byte[], byte[]> record) {
      final Object value = getValue(record);

      this.fakeKafkaService.writeRecord(record.topic(),
          FakeKafkaRecord.of(
              new Record(
                  fakeKafkaService.getTopic(record.topic()),
                  new String(record.key(), StandardCharsets.UTF_8),
                  value,
                  Optional.of(record.timestamp()),
                  null
              ),
              null)
      );
    }

    private Object getValue(final ProducerRecord<byte[], byte[]> record) {
      final Topic topic = fakeKafkaService.getTopic(record.topic());

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
