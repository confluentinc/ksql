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

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.reactivestreams.Publisher;

/**
 * The result of a query (push or pull), streamed one row at time. Records may be consumed by either
 * subscribing to the publisher or polling (blocking) for one record at a time. These two methods of
 * consumption are mutually exclusive; only one method may be used (per StreamedQueryResult).
 *
 * <p>The {@code subscribe()} method cannot be called if {@code isFailed()} is true.
 */
public interface StreamedQueryResult extends Publisher<Row> {

  List<String> columnNames();

  List<ColumnType> columnTypes();

  String queryID();

  /**
   * Block until a row becomes available.
   *
   * @return the row.
   */
  Row poll();

  /**
   * Block until a row becomes available or the timeout has elapsed.
   *
   * @param timeout amount of to wait for a row. Non-positive values are interpreted as no timeout.
   * @param timeUnit unit for timeout param.
   * @return the row, if available; else, null.
   */
  Row poll(long timeout, TimeUnit timeUnit);

  /**
   * A {@code StreamedQueryResult} is complete if the HTTP connection associated with this query has
   * been ended gracefully. Once complete, the @{code StreamedQueryResult} will continue to deliver
   * any remaining rows, then call {@code onComplete()} on the subscriber, if present.
   *
   * @return whether the {@code StreamedQueryResult} is complete.
   */
  boolean isComplete();

  /**
   * A {@code StreamedQueryResult} is failed if an error is received from the server. Once failed,
   * {@code onError()} is called on the subscriber, if present, any existing {@code poll()} calls
   * will return null, and new calls to {@code poll()} and {@code subscribe()} will be rejected.
   *
   * @return whether the {@code StreamedQueryResult} is failed.
   */
  boolean isFailed();

  void close();

}