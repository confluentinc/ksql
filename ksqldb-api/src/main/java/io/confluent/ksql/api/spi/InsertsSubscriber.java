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
 * Represents a subscriber of inserts. An implementation of this is provided by the back-end for
 * each inserts stream and it subscribes to a publisher from the API implementation. The publisher
 * then streams inserts to the back-end with back-pressure.
 */
public interface InsertsSubscriber extends Subscriber<JsonObject> {

}
