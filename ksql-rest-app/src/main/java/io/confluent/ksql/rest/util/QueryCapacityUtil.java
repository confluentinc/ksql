/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.rest.util;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;

public final class QueryCapacityUtil {
  private QueryCapacityUtil() {
  }

  public static boolean exceedsPersistentQueryCapacity(
      final KsqlEngine ksqlEngine, final KsqlConfig ksqlConfig, final long additionalQueries) {
    return (ksqlEngine.numberOfPersistentQueries() + additionalQueries) > getQueryLimit(ksqlConfig);
  }

  public static void throwTooManyActivePersistentQueriesException(
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final String statementStr) {
    throw new KsqlException(
        String.format(
            "Not executing statement(s) '%s' as it would cause the number "
                + "of active, persistent queries to exceed the configured limit. "
                + "Use the TERMINATE command to terminate existing queries, "
                + "or increase the '%s' setting via the 'ksql-server.properties' file. "
                + "Current persistent query count: %d. Configured limit: %d.",
            statementStr,
            KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG,
            ksqlEngine.numberOfPersistentQueries(),
            getQueryLimit(ksqlConfig)
        )
    );
  }

  private static int getQueryLimit(final KsqlConfig ksqlConfig) {
    return ksqlConfig.getInt(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG);
  }
}
