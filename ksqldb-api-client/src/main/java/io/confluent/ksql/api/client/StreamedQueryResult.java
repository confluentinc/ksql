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

package io.confluent.ksql.api.client;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Publisher;

/**
 * The result of a query (push or pull), streamed one row at time. Records may be consumed by either
 * subscribing to the publisher or polling (blocking) for one record at a time. These two methods of
 * consumption are mutually exclusive; only one method may be used (per StreamedQueryResult).
 *
 * <p>The {@code subscribe()} method cannot be called if {@link #isFailed} is true.
 */
public interface StreamedQueryResult extends Publisher<Row> {

  /**
   * Returns column names for these results.
   *
   * @return the column names.
   */
  List<String> columnNames();

  /**
   * Returns column types for these results.
   *
   * @return the column types.
   */
  List<ColumnType> columnTypes();

  /**
   * Returns the ID of the underlying query if the query is a push query. Else, returns null.
   *
   * @return the query ID
   */
  String queryID();

  /**
   * Returns the next row. Blocks until one is available or the underlying query is terminated
   * (either gracefully or because of an error).
   *
   * @return the row, or null if the query was terminated.
   */
  Row poll();

  /**
   * Returns the next row. Blocks until one is available, the specified timeout has elapsed, or the
   * underlying query is terminated (either gracefully or because of an error).
   *
   * @param timeout amount of time to wait for a row. A non-positive value will cause this method to
   *        block until a row is received or the query is terminated.
   * @return the row, or null if the timeout elapsed or the query was terminated.
   */
  Row poll(Duration timeout);

  /**
   * Returns whether the {@code StreamedQueryResult} is complete.
   *
   * <p>A {@code StreamedQueryResult} is complete if the HTTP connection associated with this query
   * has been ended gracefully. Once complete, the {@code StreamedQueryResult} will continue to
   * deliver any remaining rows, then call {@code onComplete()} on the subscriber, if present.
   *
   * @return whether the {@code StreamedQueryResult} is complete.
   */
  boolean isComplete();

  /**
   * Returns whether the {@code StreamedQueryResult} is failed.
   *
   * <p>A {@code StreamedQueryResult} is failed if an error is received from the server. Once
   * failed, {@code onError()} is called on the subscriber, if present, any existing {@code poll()}
   * calls will return null, and new calls to {@link #poll} and {@code subscribe()} will be
   * rejected.
   *
   * @return whether the {@code StreamedQueryResult} is failed.
   */
  boolean isFailed();

  boolean hasContinuationToken();

  AtomicReference<String> getContinuationToken();

}