package io.confluent.ksql.rest.server.services;

import io.confluent.ksql.rest.client.KsqlClient;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory.DefaultServiceContextFactory;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory.UserServiceContextFactory;
import io.confluent.ksql.services.DefaultConnectClient;
import io.confluent.ksql.services.ServiceContextFactory;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

public class TestRestServiceContextFactory {

  public interface InternalSimpleKsqlClientFactory {
    SimpleKsqlClient create(Optional<String> authHeader, KsqlClient ksqlClient);
  }

  public static DefaultServiceContextFactory createDefault(
      final InternalSimpleKsqlClientFactory ksqlClientFactory
  ) {
    return (ksqlConfig, authHeader, srClientFactory,
            connectClientFactory, sharedClient, userPrincipal) -> {
      return createUser(ksqlClientFactory).create(
          ksqlConfig,
          authHeader,
          new DefaultKafkaClientSupplier(),
          srClientFactory,
          connectClientFactory,
          sharedClient,
          userPrincipal
      );
    };
  }

  public static UserServiceContextFactory createUser(
    final InternalSimpleKsqlClientFactory ksqlClientFactory
  ) {
    return (ksqlConfig, authHeader, kafkaClientSupplier,
            srClientFactory, connectClientFactory, sharedClient, userPrincipal) -> {
      return ServiceContextFactory.create(
          ksqlConfig,
          kafkaClientSupplier,
          srClientFactory,
          () -> new DefaultConnectClient(
              ksqlConfig.getString(KsqlConfig.CONNECT_URL_PROPERTY),
              authHeader,
              Collections.emptyMap(),
              Optional.empty(),
              false),
          () -> ksqlClientFactory.create(authHeader, sharedClient)
      );
    };

  }
}
