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
import io.confluent.ksql.engine.InsertValuesEngine;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.test.serde.string.StringSerdeSupplier;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;

public class FakeInsertValuesExecutor extends InsertValuesEngine {
  private FakeKafkaService fakeKafkaService;

  public FakeInsertValuesExecutor(FakeKafkaService fakeKafkaService) {
    super();
    this.fakeKafkaService = fakeKafkaService;
  }

  public void run(
          final ConfiguredStatement<InsertValues> statement,
          final KsqlExecutionContext executionContext,
          final ServiceContext serviceContext
  ) {
    ProducerRecord<?, ?> record = buildProducerRecord(
            statement.getStatement(),
            statement.getConfig().cloneWithPropertyOverwrite(statement.getOverrides()),
            executionContext,
            serviceContext
    );

    Topic topic = this.fakeKafkaService.getTopic(record.topic());
    Object value;
    if (topic.getValueSerdeSupplier() instanceof StringSerdeSupplier) {
      value = new String((byte[]) record.value());
    } else {
      try {
        value = new ObjectMapper().readValue((byte[]) record.value(), Object.class);
      } catch (final IOException e) {
        throw new InvalidFieldException("value", "failed to parse", e);
      }
    }

    this.fakeKafkaService.writeRecord(record.topic(),
            FakeKafkaRecord.of(
            new Record(
                    topic,
                    new String((byte[]) record.key()),
                    value,
                    0,
                    null
            ),null));
  }
}
