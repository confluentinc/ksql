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

import static io.confluent.ksql.util.LimitedProxyBuilder.methodParams;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.util.LimitedProxyBuilder;
import java.util.Set;

@SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Methods invoked via reflection.
@SuppressWarnings("unused")  // Methods invoked via reflection.
public final class SandboxedKafkaConsumerGroupClient {

  static KafkaConsumerGroupClient createProxy(final KafkaConsumerGroupClient delegate) {
    return LimitedProxyBuilder.forClass(KafkaConsumerGroupClient.class)
        .forward("describeConsumerGroup", methodParams(String.class), delegate)
        .forward("listGroups", methodParams(), delegate)
        .forward("listConsumerGroupOffsets", methodParams(String.class), delegate)
        .swallow("deleteConsumerGroups", methodParams(Set.class))
        .build();
  }

  private SandboxedKafkaConsumerGroupClient() {
  }
}
