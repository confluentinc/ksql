/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server.services;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.rest.client.KsqlClient;
import io.confluent.ksql.security.KsqlPrincipal;
import io.confluent.ksql.services.ConnectClientFactory;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.ServiceContextFactory;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

public final class RestServiceContextFactory {

  private RestServiceContextFactory() {
  }

  public interface DefaultServiceContextFactory {

    ServiceContext create(
        KsqlConfig config,
        Optional<String> authHeader,
        Supplier<SchemaRegistryClient> srClientFactory,
        ConnectClientFactory connectClientFactory,
        KsqlClient sharedClient,
        List<Entry<String, String>> requestHeaders,
        Optional<KsqlPrincipal> userPrincipal
    );
  }

  public interface UserServiceContextFactory {

    ServiceContext create(
        KsqlConfig ksqlConfig,
        Optional<String> authHeader,
        KafkaClientSupplier kafkaClientSupplier,
        Supplier<SchemaRegistryClient> srClientFactory,
        ConnectClientFactory connectClientFactory,
        KsqlClient sharedClient,
        List<Entry<String, String>> requestHeaders,
        Optional<KsqlPrincipal> userPrincipal
    );
  }

  public static ServiceContext create(
      final KsqlConfig ksqlConfig,
      final Optional<String> authHeader,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final ConnectClientFactory connectClientFactory,
      final KsqlClient sharedClient,
      final List<Entry<String, String>> requestHeaders,
      final Optional<KsqlPrincipal> userPrincipal
  ) {
    return create(
        ksqlConfig,
        authHeader,
        new DefaultKafkaClientSupplier(),
        schemaRegistryClientFactory,
        connectClientFactory,
        sharedClient,
        requestHeaders,
        userPrincipal
    );
  }

  public static ServiceContext create(
      final KsqlConfig ksqlConfig,
      final Optional<String> authHeader,
      final KafkaClientSupplier kafkaClientSupplier,
      final Supplier<SchemaRegistryClient> srClientFactory,
      final ConnectClientFactory connectClientFactory,
      final KsqlClient sharedClient,
      final List<Entry<String, String>> requestHeaders,
      final Optional<KsqlPrincipal> userPrincipal
  ) {
    return ServiceContextFactory.create(
        ksqlConfig,
        kafkaClientSupplier,
        srClientFactory,
        () -> connectClientFactory.get(authHeader, requestHeaders, userPrincipal),
        () -> new DefaultKsqlClient(authHeader, sharedClient)
    );
  }


}
