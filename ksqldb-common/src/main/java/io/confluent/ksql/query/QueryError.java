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

import java.util.Objects;

/**
 * {@code QueryError} indicates additional information
 * about a query error that originated from Kafka Streams.
 */
public final class QueryError {

  private final Throwable error;
  private final Type type;

  public QueryError(final Throwable error, final Type type) {
    this.error = Objects.requireNonNull(error, "e");
    this.type = Objects.requireNonNull(type, "type");
  }

  public Throwable getError() {
    return error;
  }

  public Type getType() {
    return type;
  }

  /**
   * Specifies the type of error.
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

}
