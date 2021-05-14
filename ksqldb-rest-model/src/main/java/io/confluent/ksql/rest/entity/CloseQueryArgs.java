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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import java.util.Objects;

/**
 * Represents the arguments to a close query request
 */
@Immutable
@JsonIgnoreProperties(ignoreUnknown = true)
public class CloseQueryArgs {

  public final PushQueryId queryId;

  public CloseQueryArgs(
      final @JsonProperty(value = "queryId", required = true) PushQueryId queryId) {
    this.queryId = Objects.requireNonNull(queryId);
  }

  @Override
  public String toString() {
    return "CloseQueryArgs{"
        + "queryID='" + queryId + '\''
        + '}';
  }
}
