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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a token used to continue a push query.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PushContinuationToken {

  private final String continuationToken;

  @JsonCreator
  public PushContinuationToken(
      final @JsonProperty(value = "continuationToken", required = true) String continuationToken) {
    this.continuationToken = continuationToken;
  }

  /**
   * The token used to continue this push query.
   */
  public String getContinuationToken() {
    return continuationToken;
  }

  @Override
  public String toString() {
    return "PushContinuationToken{"
        + "continuationToken='" + continuationToken + '\''
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

    final PushContinuationToken that = (PushContinuationToken) o;
    return Objects.equals(continuationToken, that.continuationToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(continuationToken);
  }

  public static boolean isContinuationTokenEnabled(final Map<String, Object> requestProperties) {
    final Object continuationTokenEnabled
            = requestProperties.get(KsqlConfig.KSQL_QUERY_PUSH_V2_ALOS_ENABLED);

    if (continuationTokenEnabled instanceof Boolean) {
      return (boolean) continuationTokenEnabled;
    }

    return KsqlConfig.KSQL_QUERY_PUSH_V2_ALOS_ENABLED_DEFAULT;
  }
}
