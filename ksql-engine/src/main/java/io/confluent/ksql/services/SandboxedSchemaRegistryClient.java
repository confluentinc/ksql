/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.services;

import static io.confluent.ksql.services.SandboxProxyBuilder.methodParams;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.Collections;
import java.util.Objects;
import org.apache.avro.Schema;

/**
 * SchemaRegistryClient used when trying out operations.
 *
 * <p>The client will not allow any operation that changes the state of the Kafka cluster.
 *
 * <p>Most operations result in a {@code UnsupportedOperationException} being thrown as they are
 * not called.
 */
final class SandboxedSchemaRegistryClient {

  static SchemaRegistryClient createProxy(final SchemaRegistryClient delegate) {
    Objects.requireNonNull(delegate, "delegate");

    return SandboxProxyBuilder.forClass(SchemaRegistryClient.class)
        .forward("getLatestSchemaMetadata", methodParams(String.class), delegate)
        .forward("testCompatibility",
            methodParams(String.class, Schema.class), delegate)
        .swallow("deleteSubject", methodParams(String.class), Collections.emptyList())
        .build();
  }

  private SandboxedSchemaRegistryClient() {
  }
}
