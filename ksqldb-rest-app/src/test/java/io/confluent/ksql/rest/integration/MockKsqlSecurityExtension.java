package io.confluent.ksql.rest.integration;

import io.confluent.ksql.security.AuthObjectType;
import io.confluent.ksql.security.KsqlAuthTokenProvider;
import io.confluent.ksql.security.KsqlAuthorizationProvider;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.confluent.ksql.security.KsqlUserContextProvider;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.common.acl.AclOperation;
import java.security.Principal;
import java.util.List;
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
  public void initialize(final KsqlConfig ksqlConfig) {
  }

  @Override
  public Optional<KsqlAuthorizationProvider> getAuthorizationProvider() {
    return Optional.of(new KsqlAuthorizationProvider() {
      @Override
      public void checkEndpointAccess(final Principal user,
                                      final String method,
                                      final String path) {
        MockKsqlSecurityExtension.provider.checkEndpointAccess(user, method, path);
      }

      @Override
      public void checkPrivileges(final KsqlSecurityContext securityContext,
                                  final AuthObjectType objectType,
                                  final String objectName,
                                  final List<AclOperation> privileges) {
        MockKsqlSecurityExtension.provider
            .checkPrivileges(securityContext, objectType, objectName, privileges);
      }
    });
  }

  @Override
  public Optional<KsqlUserContextProvider> getUserContextProvider() {
    return Optional.empty();
  }

  @Override
  public Optional<KsqlAuthTokenProvider> getAuthTokenProvider() {
    return Optional.empty();
  }

  @Override
  public void close() {

  }
}
