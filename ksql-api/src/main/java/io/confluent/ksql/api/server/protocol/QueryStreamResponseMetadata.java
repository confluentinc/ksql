/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.server.protocol;

import io.vertx.core.json.JsonArray;

/**
 * Represents the query meta-data written initially to the response for a query stream request
 */
public class QueryStreamResponseMetadata {

  public final JsonArray columnNames;
  public final JsonArray columnTypes;
  public final String queryID;
  public final Integer rowCount;

  public QueryStreamResponseMetadata(final JsonArray columnNames, final JsonArray columnTypes,
      final String queryID, final Integer rowCount) {
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.queryID = queryID;
    this.rowCount = rowCount;
  }

  @Override
  public String toString() {
    return "QueryStreamResponseMetadata{"
        + "columnNames=" + columnNames
        + ", columnTypes=" + columnTypes
        + ", queryID='" + queryID + '\''
        + ", rowCount=" + rowCount
        + '}';
  }
}
