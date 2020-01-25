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

import com.google.errorprone.annotations.Immutable;
import io.vertx.core.json.JsonArray;
import java.util.Objects;

/**
 * Represents the metadata of a query stream response
 */
@Immutable
public class QueryResponseMetadata extends SerializableObject {

  public final String queryId;
  public final JsonArray columnNames;
  public final JsonArray columnTypes;
  public final Integer rowCount;

  public QueryResponseMetadata(final String queryId, final JsonArray columnNames,
      final JsonArray columnTypes, final Integer rowCount) {
    this.queryId = Objects.requireNonNull(queryId);
    this.columnNames = Objects.requireNonNull(columnNames);
    this.columnTypes = Objects.requireNonNull(columnTypes);
    this.rowCount = rowCount;
  }

  @Override
  public String toString() {
    return "QueryResponseMetadata{"
        + "queryId='" + queryId + '\''
        + ", columnNames=" + columnNames
        + ", columnTypes=" + columnTypes
        + ", rowcount=" + rowCount
        + '}';
  }
}
