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
import static io.confluent.ksql.util.LimitedProxyBuilder.methodParams;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.util.LimitedProxyBuilder;
import java.util.Collections;
import java.util.Objects;

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

    return LimitedProxyBuilder.forClass(SchemaRegistryClient.class)
        .swallow("register", anyParams(), 123)
        .forward("getAllSubjects", methodParams(), delegate)
        .forward("getLatestSchemaMetadata", methodParams(String.class), delegate)
        .forward("getSchemaBySubjectAndId", methodParams(String.class, int.class), delegate)
        .forward("testCompatibility",
            methodParams(String.class, ParsedSchema.class), delegate)
        .swallow("deleteSubject", methodParams(String.class), Collections.emptyList())
        .forward("getVersion", methodParams(String.class, ParsedSchema.class), delegate)
        .build();
  }

  private SandboxedSchemaRegistryClient() {
  }
}
