/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.context;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.query.QueryId;

public final class QueryLoggerUtil {

  private QueryLoggerUtil() {
  }

  /**
   * Variant of the version below that doesn't track every query individually,
   * but instead by type. This should be used for frequent query types
   * (e.g. pull queries) since each new name stores a unique logger object.
   */
  public static String queryLoggerName(final QueryType queryType, final QueryContext queryContext) {
    return String.join(
        ".",
        new ImmutableList.Builder<String>()
            .add(queryType.name().toLowerCase())
            .addAll(queryContext.getContext())
            .build()
    );
  }

  public static String queryLoggerName(final QueryId queryId, final QueryContext queryContext) {
    return String.join(
        ".",
        new ImmutableList.Builder<String>()
            .add(queryId.toString())
            .addAll(queryContext.getContext())
            .build()
    );
  }

  public enum QueryType {
    PULL_QUERY
  }
}
