/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.rest.integration;

import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.server.KsqlServerPrecondition;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class PreconditionCheckerIntegrationTestPrecondition implements KsqlServerPrecondition {

  static AtomicReference<Supplier<Optional<KsqlErrorMessage>>> ACTION
      = new AtomicReference<>(Optional::empty);

  @Override
  public Optional<KsqlErrorMessage> checkPrecondition(
      Map<String, String> properties,
      ServiceContext serviceContext,
      KafkaTopicClient internalTopicClient
  ) {
    return ACTION.get().get();
  }
}
