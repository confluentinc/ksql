/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.cli;

import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.UpgradeRejectedException;
import io.vertx.core.http.WebsocketVersion;
import io.vertx.core.net.JksOptions;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLHandshakeException;
import org.apache.kafka.common.config.SslConfigs;

public class WebsocketUtils {

  static void makeWsRequest(String request, Map<String, String> clientProps,
      TestKsqlRestApp restApp) throws SSLHandshakeException {
    makeWsRequest(request, clientProps, MultiMap.caseInsensitiveMultiMap(), restApp, true);
  }

  static void makeWsRequest(String request, Map<String, String> clientProps, MultiMap headers,
      TestKsqlRestApp restApp, boolean tls)
      throws SSLHandshakeException, UpgradeRejectedException {
    Vertx vertx = Vertx.vertx();
    io.vertx.core.http.HttpClient httpClient = null;
    try {

      HttpClientOptions httpClientOptions = new HttpClientOptions();
      if (tls) {
        httpClientOptions.setSsl(true).setVerifyHost(false);

        String trustStorePath = clientProps.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        String trustStorePassword = clientProps.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        String keyStorePath = clientProps.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        String keyStorePassword = clientProps.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);

        if (trustStorePath != null) {
          JksOptions trustStoreOptions = new JksOptions().setPath(trustStorePath);
          if (trustStorePassword != null) {
            trustStoreOptions.setPassword(trustStorePassword);
          }
          httpClientOptions.setTrustStoreOptions(trustStoreOptions);
        }

        if (keyStorePath != null) {
          JksOptions keyStoreOptions = new JksOptions().setPath(keyStorePath);
          if (keyStorePassword != null) {
            keyStoreOptions.setPassword(keyStorePassword);
          }
          httpClientOptions.setKeyStoreOptions(keyStoreOptions);
        }
      }

      httpClient = vertx.createHttpClient(httpClientOptions);

      URI listener = tls ? restApp.getWssListener() : restApp.getWsListener();

      final URI uri = listener.resolve("/ws/query?request=" + request);

      CompletableFuture<Void> completableFuture = new CompletableFuture<>();

      httpClient
          .webSocketAbs(uri.toString(), headers, WebsocketVersion.V07,
              Collections.emptyList(), ar -> {
                if (ar.succeeded()) {
                  ar.result().frameHandler(frame -> completableFuture.complete(null));
                  ar.result().exceptionHandler(completableFuture::completeExceptionally);
                } else {
                  completableFuture.completeExceptionally(ar.cause());
                }
              });
      completableFuture.get(30, TimeUnit.SECONDS);

    } catch (Exception e) {
      if (e instanceof ExecutionException) {
        if (e.getCause() instanceof SSLHandshakeException) {
          throw (SSLHandshakeException) e.getCause();
        } else if (e.getCause() instanceof UpgradeRejectedException) {
          throw (UpgradeRejectedException) e.getCause();
        } else {
          throw new RuntimeException(e);
        }
      } else {
        throw new RuntimeException(e);
      }
    } finally {
      if (httpClient != null) {
        httpClient.close();
      }
      vertx.close();
    }
  }

}
