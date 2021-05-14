/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.api.tck;

import io.confluent.ksql.reactive.BufferedPublisher;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;

public class BufferedPublisherVerificationTest extends PublisherVerification<JsonObject> {

  private final Vertx vertx;

  public BufferedPublisherVerificationTest() {
    // We need to increase the default timeouts as they are a bit low and can lead to
    // non deterministic runs
    super(new TestEnvironment(1000), 1000);
    this.vertx = Vertx.vertx();
  }

  @Override
  public Publisher<JsonObject> createPublisher(long elements) {
    final Context context = vertx.getOrCreateContext();
    List<JsonObject> initialElements = new ArrayList<>();
    if (elements < Integer.MAX_VALUE) {
      for (long l = 0; l < elements; l++) {
        initialElements.add(generateRow(l));
      }
    }
    BufferedPublisher<JsonObject> bufferedPublisher = new BufferedPublisher<>(context,
        initialElements);
    // The TCK tests want our publisher to be finite, i.e. send completion after elements
    // records have been delivered, and Long.MAX_VALUE is a special value which represents
    // infinity
    if (elements != Long.MAX_VALUE) {
      context.runOnContext(v -> bufferedPublisher.complete());
    }
    return bufferedPublisher;
  }

  @Override
  public Publisher<JsonObject> createFailedPublisher() {
    return null;
  }

  private JsonObject generateRow(long num) {
    return new JsonObject().put("id", num).put("foo", "bar");
  }


}
