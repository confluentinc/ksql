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

package io.confluent.ksql.api;

import io.confluent.ksql.api.server.BufferedPublisher;
import io.vertx.core.Context;
import io.vertx.core.json.JsonObject;

public class TestAcksPublisher extends BufferedPublisher<JsonObject> {

  private final int acksBeforePublisherError;
  private int acksSent;

  public TestAcksPublisher(final Context context, final int acksBeforePublisherError) {
    super(context);
    this.acksBeforePublisherError = acksBeforePublisherError;
  }

  @Override
  protected boolean beforeOnNext() {
    if (acksBeforePublisherError != -1 && acksSent == acksBeforePublisherError) {
      // We inject an exception after a certain number of rows
      sendError(new RuntimeException("Failure in processing"));
      return false;
    }
    acksSent++;
    return true;
  }

}
