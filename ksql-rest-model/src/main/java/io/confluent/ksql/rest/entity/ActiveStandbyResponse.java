/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@Immutable
public class ActiveStandbyResponse {

  private final ImmutableMap<String, ActiveStandbyEntity> perQueryInfo;

  @JsonCreator
  public ActiveStandbyResponse(
      @JsonProperty("perQueryInfo") final Map<String, ActiveStandbyEntity> perQueryInfo) {
    this.perQueryInfo = ImmutableMap.copyOf(requireNonNull(perQueryInfo, "perQueryInfo"));
  }

  public Map<String, ActiveStandbyEntity> getPerQueryInfo() {
    return perQueryInfo;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ActiveStandbyResponse that = (ActiveStandbyResponse) o;
    return Objects.equals(perQueryInfo, that.perQueryInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(perQueryInfo);
  }

}
