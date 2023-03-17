package io.confluent.ksql.rest.server.services;

import io.confluent.ksql.rest.client.KsqlClient;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.util.KsqlConfig;
import io.vertx.core.net.SocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Factory for creating the DefaultKsqlClient.  This is a test class that makes it easy to create
 * for functional testing, but keeps the package visibility for non test code.
 */
public class TestDefaultKsqlClientFactory {

  // Creates an instance with no auth
  public static SimpleKsqlClient instance(Map<String, Object> clientProps) {
    return new DefaultKsqlClient(Optional.empty(), clientProps, SocketAddress::inetSocketAddress);
  }

  // Creates an instance with no auth
  public static SimpleKsqlClient instance(Map<String, Object> clientProps,
      final BiFunction<Integer, String, SocketAddress> socketAddressFactory) {
    return new DefaultKsqlClient(Optional.empty(), clientProps, socketAddressFactory);
  }

  // With auth and a shared client
  public static SimpleKsqlClient instance(
      final Optional<String> authHeader,
      final Map<String, Object> clientProps,
      final KsqlClient sharedClient
  ) {
    return new DefaultKsqlClient(authHeader, sharedClient, new KsqlConfig(clientProps));
  }

}
