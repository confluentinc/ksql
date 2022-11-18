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

package io.confluent.ksql.rest.server;

import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import java.util.Optional;

public interface KsqlServerPrecondition {
  /**
   * Check a precondition for initializing the KSQL server.
   *
   * @param config The KSQL server config
   * @param serviceContext The KSQL server context for accessing external serivces
   * @return Optional.empty() if precondition check passes, non-empty KsqlErrorMessage object if the
   *         check does not pass.
   */
  Optional<KsqlErrorMessage> checkPrecondition(
      KsqlRestConfig config,
      ServiceContext serviceContext,
      KafkaTopicClient internalTopicClient
  );
}
