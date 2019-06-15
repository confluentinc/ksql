/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.execution;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.engine.InsertValuesEngine;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.LongSupplier;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;

public class InsertValuesExecutor extends InsertValuesEngine {
  private static final long MAX_SEND_TIMEOUT_SECONDS = 5;

  public InsertValuesExecutor() {
    super();
  }

  @VisibleForTesting
  InsertValuesExecutor(final LongSupplier clock) {
    super(clock);
  }

  public void run(
          final ConfiguredStatement<InsertValues> statement,
          final KsqlExecutionContext executionContext,
          final ServiceContext serviceContext
  ) {
    if (!statement.getConfig().getBoolean(KsqlConfig.KSQL_INSERT_INTO_VALUES_ENABLED)) {
      throw new KsqlException("The server has disabled INSERT INTO ... VALUES functionality. "
              + "To enable it, restart your KSQL-server with 'ksql.insert.into.values.enabled'=true");
    }
    final InsertValues insertValues = statement.getStatement();
    final KsqlConfig config = statement.getConfig()
            .cloneWithPropertyOverwrite(statement.getOverrides());

    // for now, just create a new producer each time
    final Producer<byte[], byte[]> producer = serviceContext
            .getKafkaClientSupplier()
            .getProducer(config.getProducerClientConfigProps());

    final Future<RecordMetadata> producerCallResult = producer.send(
            buildProducerRecord(insertValues, config, executionContext, serviceContext));

    producer.close(Duration.ofSeconds(MAX_SEND_TIMEOUT_SECONDS));

    try {
      // Check if the producer failed to write to the topic. This can happen if the
      // ServiceContext does not have write permissions.
      producerCallResult.get();
    } catch (final Exception e) {
      throw new KsqlException("Failed to insert values into stream/table: "
              + insertValues.getTarget().getSuffix(), e);
    }
  }

  public Optional<KsqlEntity> execute(
          final ConfiguredStatement<InsertValues> statement,
          final KsqlExecutionContext executionContext,
          final ServiceContext serviceContext
  ) {
    run(statement, executionContext, serviceContext);
    return Optional.empty();
  }
}

