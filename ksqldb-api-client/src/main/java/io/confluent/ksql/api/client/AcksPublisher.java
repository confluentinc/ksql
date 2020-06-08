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

import org.reactivestreams.Publisher;

/**
 * TODO
 */
public interface AcksPublisher extends Publisher<InsertAck> {

  /**
   * Returns whether the {@code AcksPublisher} is complete.
   *
   * <p>An {@code AcksPublisher} is complete if the HTTP connection associated with this
   * {@code insertsStream()} request has been ended gracefully. Once complete, the
   * {@code AcksPublisher} will continue to deliver any remaining rows, then call
   * {@code onComplete()} on the subscriber, if present.
   *
   * @return whether the {@code AcksPublisher} is complete.
   */
  boolean isComplete();

  /**
   * Returns whether the {@code AcksPublisher} is failed.
   *
   * <p>An {@code AcksPublisher} is failed if an error is received from the server. Once
   * failed, {@code onError()} is called on the subscriber, if present, and new calls to
   * {@code subscribe()} will be rejected.
   *
   * @return whether the {@code AcksPublisher} is failed.
   */
  boolean isFailed();

}
