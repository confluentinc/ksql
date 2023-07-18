/*
 * Copyright 2021 Confluent Inc.
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 * http://www.confluent.io/confluent-community-license
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.logging.query;

import io.confluent.ksql.util.QueryGuid;

public final class QueryLoggerMessage {
  private Object message;
  private String query;
  private QueryGuid queryGuid;

  public QueryLoggerMessage(final Object message, final String query) {
    this.message = message;
    this.query = query;
  }

  public QueryLoggerMessage(final Object message, final String query, final QueryGuid guid) {
    this.message = message;
    this.query = query;
    this.queryGuid = guid;
  }

  public Object getMessage() {
    return message;
  }

  public String getQuery() {
    return query;
  }

  public String toString() {
    if (queryGuid == null) {
      return String.format("%s: %s", message, query);
    }

    return String.format("%s (%s): %s", message, queryGuid.getQueryGuid(), query);
  }

  public QueryGuid getQueryIdentifier() {
    return queryGuid;
  }
}
