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
   * @param sql        The sql of the query
   * @param push       If true then push query otherwise pull query
   * @param properties Optional properties for the query
   * @return The publisher
   */
  QueryPublisher createQueryPublisher(String sql, boolean push, JsonObject properties);

  /**
   * Create a subscriber which will receive a stream of inserts from the API server and process
   * them. This method takes an optional acksSubsciber - if specified this is used to receive a
   * stream of acks from the back-end representing completion of the processing of the inserts
   *
   * @param target         The target to insert into
   * @param properties     Optional properties
   * @param acksSubscriber Optional subscriber of acks
   * @return The inserts subscriber
   */
  InsertsSubscriber createInsertsSubscriber(String target, JsonObject properties,
      Subscriber<JsonObject> acksSubscriber);

}
