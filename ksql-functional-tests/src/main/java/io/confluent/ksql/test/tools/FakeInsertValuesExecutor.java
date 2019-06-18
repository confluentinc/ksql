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

package io.confluent.ksql.test.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.engine.InsertValuesHandler;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.test.serde.string.StringSerdeSupplier;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.util.KsqlConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.kafka.clients.producer.ProducerRecord;

public final class FakeInsertValuesExecutor extends InsertValuesHandler {
  private final FakeKafkaService fakeKafkaService;

  private FakeInsertValuesExecutor(final FakeKafkaService fakeKafkaService) {
    super();
    this.fakeKafkaService = Objects.requireNonNull(fakeKafkaService, "fakeKafkaService");
  }

  public static FakeInsertValuesExecutor of(final FakeKafkaService fakeKafkaService) {
    return new FakeInsertValuesExecutor(fakeKafkaService);
  }

  @Override
  protected void sendRecord(
          final ProducerRecord<byte[],byte[]> record,
          final InsertValues insertValues,
          final KsqlConfig config,
          final KsqlExecutionContext executionContext,
          final ServiceContext serviceContext
  ) {
    final Topic topic = this.fakeKafkaService.getTopic(record.topic());
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

    this.fakeKafkaService.writeRecord(record.topic(),
            FakeKafkaRecord.of(
                    new Record(
                            topic,
                            new String(record.key(), StandardCharsets.UTF_8),
                            value,
                            record.timestamp(),
                            null
                    ),
                    null)
    );
  }
}
