/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.util;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlStatementException;

public final class QueryCapacityUtil {
  private QueryCapacityUtil() {
  }

  public static boolean exceedsPersistentQueryCapacity(
      final KsqlExecutionContext executionContext,
      final KsqlConfig ksqlConfig
  ) {
    return executionContext.getPersistentQueries().size() > getQueryLimit(ksqlConfig);
  }

  public static void throwTooManyActivePersistentQueriesException(
      final KsqlExecutionContext executionContext,
      final KsqlConfig ksqlConfig,
      final String statementStr
  ) {
    final String sanitizedMessage = String.format(
        "Not executing statement(s) as it would cause the number "
            + "of active, persistent queries to exceed the configured limit. "
            + "Use the TERMINATE command to terminate existing queries, "
            + "or increase the '%s' setting via the 'ksql-server.properties' file. "
            + "Current persistent query count: %d. Configured limit: %d.",
        KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG,
        executionContext.getPersistentQueries().size(),
        getQueryLimit(ksqlConfig)
    );
    final String unloggedMessage = String.format(
        "Not executing statement(s) '%s' as it would cause the number "
            + "of active, persistent queries to exceed the configured limit. "
            + "Use the TERMINATE command to terminate existing queries, "
            + "or increase the '%s' setting via the 'ksql-server.properties' file. "
            + "Current persistent query count: %d. Configured limit: %d.",
        statementStr,
        KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG,
        executionContext.getPersistentQueries().size(),
        getQueryLimit(ksqlConfig)
    );
    throw new KsqlStatementException(
        sanitizedMessage,
        unloggedMessage,
        statementStr,
        KsqlStatementException.Problem.REQUEST
    );
  }

  private static int getQueryLimit(final KsqlConfig ksqlConfig) {
    return ksqlConfig.getInt(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG);
  }

  public static boolean exceedsPushQueryCapacity(
          final KsqlExecutionContext executionContext,
          final KsqlRestConfig ksqlRestConfig
  ) {
    return getNumLivePushQueries(executionContext) >= getPushQueryLimit(ksqlRestConfig);
  }

  public static void throwTooManyActivePushQueriesException(
          final KsqlExecutionContext executionContext,
          final KsqlRestConfig ksqlRestConfig,
          final String statementStr
  ) {
    final String sanitizedMessage = String.format(
        "Not executing statement(s) as it would cause the number "
            + "of active, push queries to exceed the configured limit. "
            + "Terminate existing PUSH queries, "
            + "or increase the '%s' setting via the 'ksql-server.properties' file. "
            + "Current push query count: %d. Configured limit: %d.",
        KsqlRestConfig.MAX_PUSH_QUERIES,
        getNumLivePushQueries(executionContext),
        getPushQueryLimit(ksqlRestConfig)
    );
    final String unloggedMessage = String.format(
        "Not executing statement(s) '%s' as it would cause the number "
            + "of active, push queries to exceed the configured limit. "
            + "Terminate existing PUSH queries, "
            + "or increase the '%s' setting via the 'ksql-server.properties' file. "
            + "Current push query count: %d. Configured limit: %d.",
        statementStr,
        KsqlRestConfig.MAX_PUSH_QUERIES,
        getNumLivePushQueries(executionContext),
        getPushQueryLimit(ksqlRestConfig)
    );
    throw new KsqlStatementException(
        sanitizedMessage,
        unloggedMessage,
        statementStr
    );
  }

  private static int getNumLivePushQueries(final KsqlExecutionContext ctx) {
    return ctx.getAllLiveQueries().size() - ctx.getPersistentQueries().size();
  }

  private static int getPushQueryLimit(final KsqlRestConfig ksqlRestConfig) {
    return ksqlRestConfig.getInt(KsqlRestConfig.MAX_PUSH_QUERIES);
  }

}
