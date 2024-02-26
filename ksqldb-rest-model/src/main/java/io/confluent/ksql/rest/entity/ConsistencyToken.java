/*
 * Copyright 2019 Confluent Inc.
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Represents a consistency token created by serializing a ConsistencyOffsetVector.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsistencyToken {
  private final String consistencyToken;

  @JsonCreator
  public ConsistencyToken(
      final @JsonProperty(value = "consistencyToken", required = true) String consistencyToken) {
    this.consistencyToken = consistencyToken;
  }

  public String getConsistencyToken() {
    return consistencyToken;
  }

  @Override
  public String toString() {
    return "ConsistencyToken{"
        + "consistencyToken='" + consistencyToken + '\''
        + '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ConsistencyToken that = (ConsistencyToken) o;
    return Objects.equals(consistencyToken, that.consistencyToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(consistencyToken);
  }

}
