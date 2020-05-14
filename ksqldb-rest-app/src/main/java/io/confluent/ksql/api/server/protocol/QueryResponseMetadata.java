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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import java.util.List;
import java.util.Objects;

/**
 * Represents the metadata of a query stream response
 */
@Immutable
public class QueryResponseMetadata extends AbstractSerializableObject {

  public final String queryId;
  public final ImmutableList<String> columnNames;
  public final ImmutableList<String> columnTypes;

  public QueryResponseMetadata(
      final @JsonProperty(value = "queryId") String queryId,
      final @JsonProperty(value = "columnNames") List<String> columnNames,
      final @JsonProperty(value = "columnTypes") List<String> columnTypes) {
    this.queryId = queryId;
    this.columnNames = ImmutableList.copyOf(Objects.requireNonNull(columnNames));
    this.columnTypes = ImmutableList.copyOf(Objects.requireNonNull(columnTypes));
  }

  public QueryResponseMetadata(final List<String> columnNames, final List<String> columnTypes) {
    this(null, columnNames, columnTypes);
  }

}
