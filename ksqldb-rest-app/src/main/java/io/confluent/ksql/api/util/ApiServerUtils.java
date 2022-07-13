/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.api.util;

import io.confluent.ksql.properties.PropertiesUtil;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.util.QueryMask;
import io.confluent.ksql.util.VertxSslOptionsFactory;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.PfxOptions;
import io.vertx.ext.web.RoutingContext;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ApiServerUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ApiServerUtils.class);

  private ApiServerUtils() {
  }

  public static void setMaskedSqlIfNeeded(final KsqlRequest request) {
    try {
      request.getMaskedKsql();
    } catch (final Exception e) {
      ApiServerUtils.setMaskedSql(request);
    }
  }

  public static void setMaskedSql(final KsqlRequest request) {
    request.setMaskedKsql(QueryMask.getMaskedStatement(request.getUnmaskedKsql()));
  }

  public static void unhandledExceptionHandler(final Throwable t) {
    if (t instanceof ClosedChannelException) {
      LOG.debug("Unhandled ClosedChannelException (connection likely closed early)", t);
    } else {
      LOG.error("Unhandled exception", t);
    }
  }

  public static void chcHandler(final RoutingContext routingContext) {
    routingContext.response().putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
        .end(new JsonObject().toBuffer());
  }

  public static List<URI> parseListenerStrings(
      final KsqlRestConfig config,
      final List<String> stringListeners) {
    final List<URI> listeners = new ArrayList<>();
    for (String listenerName : stringListeners) {
      try {
        final URI uri = new URI(listenerName);
        final String scheme = uri.getScheme();
        if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
          throw new ConfigException("Invalid URI scheme should be http or https: " + listenerName);
        }
        if ("https".equalsIgnoreCase(scheme)) {
          final String keyStoreLocation = config
              .getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
          if (keyStoreLocation == null || keyStoreLocation.isEmpty()) {
            throw new ConfigException("https listener specified but no keystore provided");
          }
        }
        listeners.add(uri);
      } catch (URISyntaxException e) {
        throw new ConfigException("Invalid listener URI: " + listenerName);
      }
    }
    return listeners;
  }

  public static List<URI> parseListeners(final KsqlRestConfig config) {
    final List<String> sListeners = config.getList(KsqlRestConfig.LISTENERS_CONFIG);
    return parseListenerStrings(config, sListeners);
  }

  private static void configureTlsKeyStore(
      final KsqlRestConfig ksqlRestConfig,
      final HttpServerOptions httpServerOptions,
      final String keyStoreAlias
  ) {
    final Map<String, String> props = PropertiesUtil.toMapStrings(ksqlRestConfig.originals());
    final String keyStoreType = ksqlRestConfig.getString(KsqlRestConfig.SSL_KEYSTORE_TYPE_CONFIG);

    if (keyStoreType.equals(KsqlRestConfig.SSL_STORE_TYPE_JKS)) {
      final Optional<JksOptions> keyStoreOptions =
          VertxSslOptionsFactory.buildJksKeyStoreOptions(props, Optional.ofNullable(keyStoreAlias));

      keyStoreOptions.ifPresent(options -> httpServerOptions.setKeyStoreOptions(options));
    } else if (keyStoreType.equals(KsqlRestConfig.SSL_STORE_TYPE_PKCS12)) {
      final Optional<PfxOptions> keyStoreOptions =
          VertxSslOptionsFactory.getPfxKeyStoreOptions(props);

      keyStoreOptions.ifPresent(options -> httpServerOptions.setPfxKeyCertOptions(options));
    }
  }

  private static void configureTlsTrustStore(
      final KsqlRestConfig ksqlRestConfig,
      final HttpServerOptions httpServerOptions
  ) {
    final Map<String, String> props = PropertiesUtil.toMapStrings(ksqlRestConfig.originals());
    final String trustStoreType =
        ksqlRestConfig.getString(KsqlRestConfig.SSL_TRUSTSTORE_TYPE_CONFIG);

    if (trustStoreType.equals(KsqlRestConfig.SSL_STORE_TYPE_JKS)) {
      final Optional<JksOptions> trustStoreOptions =
          VertxSslOptionsFactory.getJksTrustStoreOptions(props);

      trustStoreOptions.ifPresent(options -> httpServerOptions.setTrustOptions(options));
    } else if (trustStoreType.equals(KsqlRestConfig.SSL_STORE_TYPE_PKCS12)) {
      final Optional<PfxOptions> trustStoreOptions =
          VertxSslOptionsFactory.getPfxTrustStoreOptions(props);

      trustStoreOptions.ifPresent(options -> httpServerOptions.setTrustOptions(options));
    }
  }
}
