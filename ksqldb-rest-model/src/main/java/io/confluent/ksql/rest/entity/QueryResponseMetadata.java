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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents the metadata of a query stream response
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryResponseMetadata {

  public final String queryId;
  public final List<String> columnNames;
  public final List<String> columnTypes;

  // Just used to pass data around rather than directly serialized in a response
  @JsonIgnore
  public final LogicalSchema schema;

  public QueryResponseMetadata(
      final @JsonProperty(value = "queryId") String queryId,
      final @JsonProperty(value = "columnNames") List<String> columnNames,
      final @JsonProperty(value = "columnTypes") List<String> columnTypes,
      final @JsonProperty(value = "schema") LogicalSchema schema) {
    this.queryId = queryId;
    this.columnNames = Collections.unmodifiableList(Objects.requireNonNull(columnNames));
    this.columnTypes = Collections.unmodifiableList(Objects.requireNonNull(columnTypes));
    this.schema = schema;
  }

  public QueryResponseMetadata(final List<String> columnNames, final List<String> columnTypes,
      final LogicalSchema schema) {
    this(null, columnNames, columnTypes, schema);
  }

}
