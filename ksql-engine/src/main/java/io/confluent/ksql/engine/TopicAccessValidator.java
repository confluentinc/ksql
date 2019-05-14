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

import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlException;

/**
 * Checks if a {@link ServiceContext} has access to all source and target topics of a
 * KSQL statement.
 */
public interface TopicAccessValidator {
  /**
   * Checks if the {@link ServiceContext} has access to the source and target topics of transient
   * and persistent query statements.
   * </p>
   * It checks for Read access to all the source topics and Write access to the target topic.
   * </p>
   * Other permissions checks are already validated by the {@code Injectors} which attempts to
   * create and/or delete topics using the same {@code ServiceContext}.
   *
   * @param statement The statement to verify for permissions.
   * @throws KsqlException If a topic is not authorized for access, or the topic does not exist.
   */
  void validate(ServiceContext serviceContext, Statement statement);
}
