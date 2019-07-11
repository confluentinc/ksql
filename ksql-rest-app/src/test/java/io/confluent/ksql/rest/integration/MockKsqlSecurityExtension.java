package io.confluent.ksql.rest.integration;

import io.confluent.ksql.rest.server.security.KsqlAuthorizationProvider;
import io.confluent.ksql.rest.server.security.KsqlSecurityExtension;
import io.confluent.ksql.rest.server.security.KsqlUserContextProvider;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;

/**
 * Mock the Security extension and authorization provider for all tests
 */
public class MockKsqlSecurityExtension implements KsqlSecurityExtension {
  private static KsqlAuthorizationProvider provider;

  public static void setAuthorizationProvider(final KsqlAuthorizationProvider provider) {
    MockKsqlSecurityExtension.provider = provider;
  }

  @Override
  public void initialize(KsqlConfig ksqlConfig) {
  }

  @Override
  public Optional<KsqlAuthorizationProvider> getAuthorizationProvider() {
    return Optional.of((user, method, path) ->
        MockKsqlSecurityExtension.provider.checkEndpointAccess(user, method, path));
  }

  @Override
  public Optional<KsqlUserContextProvider> getUserContextProvider() {
    return Optional.empty();
  }

  @Override
  public void close() {

  }
}
