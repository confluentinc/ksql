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

package io.confluent.ksql.security;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.services.ServiceContext;

/**
 * Checks if a user (configured in the {@link ServiceContext}) has permissions to execute the
 * specified KSQL {@link Statement}.
 */
public interface KsqlAuthorizationValidator {
  /**
   * Checks if a user (configured in the {@link ServiceContext}) has permissions to execute the
   * specified KSQL {@link Statement}.
   *
   * @param serviceContext The service context to validate Kafka/SR authorization.
   * @param metaStore The metastore object to obtain extra statement metadata.
   * @param statement The statement to check for authorization.
   */
  void checkAuthorization(ServiceContext serviceContext, MetaStore metaStore, Statement statement);
}
