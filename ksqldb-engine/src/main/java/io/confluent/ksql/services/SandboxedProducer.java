/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.services;

import static io.confluent.ksql.util.LimitedProxyBuilder.anyParams;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.util.LimitedProxyBuilder;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.Producer;

/**
 * A limited producer that can be used while trying out operations.
 *
 * <p>The client will not allow any operation that changes the state of the Kafka cluster.
 *
 * <p>Most operations result in a {@code UnsupportedOperationException} being thrown as they are
 * not called.
 */
final class SandboxedProducer<K, V> {

  @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION") // `null` is valid for `completedFuture`.
  static <K, V> Producer<K, V> createProxy() {
    return LimitedProxyBuilder.forClass(Producer.class)
        .swallow("send", anyParams(), CompletableFuture.completedFuture(null))
        .swallow("close", anyParams())
        .swallow("registerMetricForSubscription", anyParams())
        .swallow("unregisterMetricFromSubscription", anyParams())
        .build();
  }

  private SandboxedProducer() {
  }
}
