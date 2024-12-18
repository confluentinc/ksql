/*
 * Copyright 2022 Confluent Inc.
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

import io.vertx.core.json.JsonObject;
import java.util.concurrent.CompletableFuture;

public final class AssertResponseHandler {

  private AssertResponseHandler() {

  }

  static void handleAssertSchemaResponse(
      final JsonObject assertSchemaResponse,
      final CompletableFuture<Void> cf
  ) {
    if (assertSchemaResponse.getBoolean("exists") != null
        && assertSchemaResponse.containsKey("subject")
        && assertSchemaResponse.containsKey("id")
    ) {
      cf.complete(null);
    } else {
      cf.completeExceptionally(new IllegalStateException(
          "Unexpected server response format. Response: " + assertSchemaResponse
      ));
    }
  }

  static void handleAssertTopicResponse(
      final JsonObject assertTopicResponse,
      final CompletableFuture<Void> cf
  ) {
    if (assertTopicResponse.getBoolean("exists") != null
        && assertTopicResponse.getString("topicName") != null
    ) {
      cf.complete(null);
    } else {
      cf.completeExceptionally(new IllegalStateException(
          "Unexpected server response format. Response: " + assertTopicResponse
      ));
    }
  }
}
