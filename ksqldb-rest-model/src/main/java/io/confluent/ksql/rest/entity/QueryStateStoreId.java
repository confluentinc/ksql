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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Immutable;
import java.util.Objects;

/**
 * This represents a unique store in the system and the basis for getting lag information from
 * KafkaStreams, exposed by the QueryMetadata.
 */
@Immutable
@JsonIgnoreProperties(ignoreUnknown = true)
public final class QueryStateStoreId {
  private static final String SEPARATOR = "#";

  private final String queryId;
  private final String stateStoreName;

  public static QueryStateStoreId of(final String queryId, final String stateStoreName) {
    return new QueryStateStoreId(queryId, stateStoreName);
  }

  public static QueryStateStoreId of(final String serializedKey) {
    return new QueryStateStoreId(serializedKey);
  }

  private QueryStateStoreId(final String queryId, final String stateStoreName) {
    this.queryId = queryId;
    this.stateStoreName = stateStoreName;
  }

  @JsonCreator
  public QueryStateStoreId(final String serializedPair) {
    final String [] parts = serializedPair.split("\\" + SEPARATOR);
    Preconditions.checkArgument(parts.length == 2);
    this.queryId = Objects.requireNonNull(parts[0]);
    this.stateStoreName = Objects.requireNonNull(parts[1]);
  }

  public String getQueryId() {
    return queryId;
  }

  public String getStateStoreName() {
    return stateStoreName;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final QueryStateStoreId that = (QueryStateStoreId) o;
    return Objects.equals(queryId, that.queryId)
        && Objects.equals(stateStoreName, that.stateStoreName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(queryId, stateStoreName);
  }

  @JsonValue
  @Override
  public String toString() {
    return queryId + SEPARATOR + stateStoreName;
  }
}
