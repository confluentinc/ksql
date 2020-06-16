package io.confluent.ksql.rest.server.services;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.rest.client.KsqlClient;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.util.KeystoreUtil;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.SocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.common.config.SslConfigs;

public class InternalKsqlClientFactory {

  public static KsqlClient createInternalClient(
      final Map<String, String> clientProps,
      final BiFunction<Integer, String, SocketAddress> socketAddressFactory,
      final Supplier<Vertx> vertxSupplier) {
    final String internalClientAuth = clientProps.get(
        KsqlRestConfig.KSQL_INTERNAL_SSL_CLIENT_AUTHENTICATION_CONFIG);
    final boolean verifyHost = !Strings.isNullOrEmpty(internalClientAuth)
        && !KsqlRestConfig.SSL_CLIENT_AUTHENTICATION_NONE.equals(internalClientAuth);

    return new KsqlClient(
        Optional.empty(),
        new LocalProperties(ImmutableMap.of()),
        httpOptionsFactory(clientProps, verifyHost),
        socketAddressFactory,
        vertxSupplier
    );
  }

  private static Function<Boolean, HttpClientOptions> httpOptionsFactory(
      final Map<String, String> clientProps, final boolean verifyHost) {
    return (tls) -> {
      final HttpClientOptions httpClientOptions = createClientOptions();
      if (!tls) {
        return httpClientOptions;
      }
      httpClientOptions.setVerifyHost(verifyHost);
      httpClientOptions.setSsl(true);
      final String trustStoreLocation = clientProps.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
      if (!Strings.isNullOrEmpty(trustStoreLocation)) {
        final String suppliedTruststorePassword = clientProps
            .get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        httpClientOptions.setTrustStoreOptions(new JksOptions().setPath(trustStoreLocation)
            .setPassword(Strings.nullToEmpty(suppliedTruststorePassword)));

        final String keyStoreLocation = clientProps.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        if (!Strings.isNullOrEmpty(keyStoreLocation)) {
          final String suppliedKeyStorePassword = clientProps
              .get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
          final String internalAlias = clientProps
              .get(KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_INTERNAL_CONFIG);
          final JksOptions keyStoreOptions = new JksOptions()
              .setPassword(Strings.nullToEmpty(suppliedKeyStorePassword));
          if (!Strings.isNullOrEmpty(internalAlias)) {
            keyStoreOptions.setValue(KeystoreUtil.getKeyStore(
                KsqlRestConfig.SSL_STORE_TYPE_JKS,
                keyStoreLocation,
                Optional.ofNullable(Strings.emptyToNull(suppliedKeyStorePassword)),
                Optional.ofNullable(Strings.emptyToNull(suppliedKeyStorePassword)),
                internalAlias));
          } else {
            keyStoreOptions.setPath(keyStoreLocation);
          }
          httpClientOptions.setKeyStoreOptions(keyStoreOptions);
        }
      }
      return httpClientOptions;
    };
  }

  private static HttpClientOptions createClientOptions() {
    return new HttpClientOptions().setMaxPoolSize(100);
  }
}
