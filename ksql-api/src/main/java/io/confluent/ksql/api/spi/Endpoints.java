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

package io.confluent.ksql.api.spi;

import io.confluent.ksql.api.server.InsertResult;
import io.vertx.core.Context;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import org.reactivestreams.Subscriber;

/**
 * In order to keep a clean separation between the plumbing of the API server and actual back-end
 * implementation of the endpoints we define this interface to encapsulate the actual endpoint
 * implementation.
 */
public interface Endpoints {

  /**
   * Create a publisher that will publish the results of the query. This will be subscribed to from
   * the API server and the subscriber will write the results to the HTTP response
   *
   * @param sql            The sql of the query
   * @param properties     Optional properties for the query
   * @param context        The Vert.x context
   * @param workerExecutor The worker executor to use for blocking operations
   * @return The publisher
   */
  QueryPublisher createQueryPublisher(String sql, JsonObject properties,
      Context context, WorkerExecutor workerExecutor);

  /**
   * Create a subscriber which will receive a stream of inserts from the API server and process
   * them. This method takes an optional acksSubsciber - if specified this is used to receive a
   * stream of acks from the back-end representing completion of the processing of the inserts
   *
   * @param target         The target to insert into
   * @param properties     Optional properties
   * @param acksSubscriber Optional subscriber of acks
   * @param context        The Vert.x context
   * @return The inserts subscriber
   */
  Subscriber<JsonObject> createInsertsSubscriber(String target, JsonObject properties,
      Subscriber<InsertResult> acksSubscriber, Context context);

}
