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
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.properties.PropertyOverrider;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.Optional;

public final class PropertyExecutor {

  private PropertyExecutor() { }

  public static Optional<KsqlEntity> set(
      final ConfiguredStatement<SetProperty> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    PropertyOverrider.set(statement);
    return Optional.empty();
  }

  public static Optional<KsqlEntity> unset(
      final ConfiguredStatement<UnsetProperty> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    PropertyOverrider.unset(statement);
    return Optional.empty();
  }

}
