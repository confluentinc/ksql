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

package io.confluent.ksql.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * {@code QueryError} indicates additional information
 * about a query error that originated from Kafka Streams.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class QueryError {

  private final long timestamp;
  private final String errorMessage;
  private final Type type;

  @JsonCreator
  public QueryError(
      @JsonProperty("timestamp") final long timestamp,
      @JsonProperty("errorMessage") final String errorMessage,
      @JsonProperty("type") final Type type
  ) {
    this.errorMessage = Objects.requireNonNull(errorMessage, "errorMessage");
    this.type = Objects.requireNonNull(type, "type");
    this.timestamp = timestamp;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public Type getType() {
    return type;
  }

  /**
   * Specifies the type of error.
   * The ordinal values of the Type enum are used as the metrics values.
   * Please ensure preservation of the current order.
   */
  public enum Type {
    /**
     * An error that can not be classified as any of the below.
     */
    UNKNOWN,

    /**
     * A {@code USER} error is something that is not recoverable
     * by simple retries. Common errors in this category include
     * out-of-band topic deletion or bad ACL configuration.
     */
    USER,

    /**
     * A {@code SYSTEM} error occurs when something in the
     * environment causes the query to fail. This could be caused
     * by timeouts when attempting to access the Kafka broker while
     * it is temporarily unavailable. Retrying these queries are
     * likely to remediate the issue.
     */
    SYSTEM
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final QueryError that = (QueryError) o;
    return Objects.equals(errorMessage, that.errorMessage)
        && type == that.type
        && timestamp == that.timestamp;
  }

  @Override
  public int hashCode() {
    return Objects.hash(errorMessage, type, timestamp);
  }
}
