package io.confluent.ksql.rest.server.services;

import io.confluent.ksql.services.SimpleKsqlClient;
import java.util.Optional;

/**
 * Factory for creating the DefaultKsqlClient.  This is a test class that makes it easy to create
 * for functional testing, but keeps the package visibility for non test code.
 */
public class TestDefaultKsqlClientFactory {

  // Creates an instance with no auth
  public static SimpleKsqlClient instance() {
    return new DefaultKsqlClient(Optional.empty());
  }

  // Creates an instance with the given auth headers
  public static SimpleKsqlClient instance(final Optional<String> authHeader) {
    return new DefaultKsqlClient(authHeader);
  }

}
