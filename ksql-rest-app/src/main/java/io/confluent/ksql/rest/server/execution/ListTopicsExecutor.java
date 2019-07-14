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

package io.confluent.ksql.rest.server.execution;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.rest.entity.KafkaTopicsList;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KafkaConsumerGroupClient;
import io.confluent.ksql.util.KafkaConsumerGroupClientImpl;
import java.util.Optional;

public final class ListTopicsExecutor {

  private ListTopicsExecutor() { }

  public static Optional<KsqlEntity> execute(
      final ConfiguredStatement<ListTopics> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final KafkaTopicClient client = serviceContext.getTopicClient();
    final KafkaConsumerGroupClient kafkaConsumerGroupClient
        = new KafkaConsumerGroupClientImpl(serviceContext.getAdminClient());

    return Optional.of(KafkaTopicsList.build(
        statement.getStatementText(),
        client.describeTopics(client.listNonInternalTopicNames()),
        statement.getConfig(),
        kafkaConsumerGroupClient
    ));
  }
}
