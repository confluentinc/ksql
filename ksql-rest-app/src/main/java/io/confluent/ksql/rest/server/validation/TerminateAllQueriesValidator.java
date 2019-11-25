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

package io.confluent.ksql.rest.server.validation;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.tree.TerminateAllQueries;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Map;

public final class TerminateAllQueriesValidator {

  private TerminateAllQueriesValidator() {
  }

  @SuppressWarnings("unused") // not used, but required to match interface
  public static void validate(
      final ConfiguredStatement<TerminateAllQueries> statement,
      final Map<String, Object> mutableScopedProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    executionContext.getPersistentQueries().forEach(QueryMetadata::close);
  }
}
