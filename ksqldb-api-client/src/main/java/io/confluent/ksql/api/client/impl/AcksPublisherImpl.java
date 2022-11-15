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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.client.AcksPublisher;
import io.confluent.ksql.api.client.InsertAck;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.vertx.core.Context;

public class AcksPublisherImpl extends BufferedPublisher<InsertAck> implements AcksPublisher {

  public AcksPublisherImpl(final Context context) {
    super(context);
  }

  @Override
  public boolean isComplete() {
    return super.isComplete();
  }

  @Override
  public boolean isFailed() {
    return super.isFailed();
  }

  public void handleError(final Exception e) {
    sendError(e);
  }
}
