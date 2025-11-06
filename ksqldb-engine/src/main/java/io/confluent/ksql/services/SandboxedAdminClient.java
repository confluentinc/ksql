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

import io.confluent.ksql.util.LimitedProxyBuilder;
import org.apache.kafka.clients.admin.Admin;

/**
 * An admin client to use while trying out operations.
 *
 * <p>The client will not make changes to the remote Kafka cluster.
 *
 * <p>Most operations result in a {@code UnsupportedOperationException} being thrown as they are
 * not called.
 */
final class SandboxedAdminClient {

  static Admin createProxy() {
    return LimitedProxyBuilder.forClass(Admin.class)
        .swallow("close", anyParams())
        .swallow("registerMetricForSubscription", anyParams())
        .swallow("unregisterMetricFromSubscription", anyParams())
        .build();
  }

  private SandboxedAdminClient() {
  }
}
