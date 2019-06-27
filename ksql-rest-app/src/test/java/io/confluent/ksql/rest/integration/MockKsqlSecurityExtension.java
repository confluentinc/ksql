package io.confluent.ksql.rest.integration;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.rest.server.security.KsqlAuthorizationProvider;
import io.confluent.ksql.rest.server.security.KsqlSecurityExtension;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.streams.KafkaClientSupplier;

import javax.ws.rs.core.Configurable;
import java.security.Principal;
import java.util.Optional;
import java.util.function.Supplier;

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
  public void register(Configurable<?> configurable) {
  }

  @Override
  public KafkaClientSupplier getKafkaClientSupplier(Principal principal) throws KsqlException {
    return null;
  }

  @Override
  public Supplier<SchemaRegistryClient> getSchemaRegistryClientSupplier(Principal principal) throws KsqlException {
    return null;
  }

  @Override
  public void close() {

  }
}
