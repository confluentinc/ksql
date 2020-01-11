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

  QueryPublisher createQueryPublisher(String sql, boolean push, JsonObject properties);

  InsertsSubscriber createInsertsSubscriber(String target, JsonObject properties,
      Subscriber<Void> acksSubscriber);

}
