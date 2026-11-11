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

import io.confluent.ksql.rest.client.KsqlClient;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.util.VertxSslOptionsFactory;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.UpgradeRejectedException;
import io.vertx.core.http.WebSocketClient;
import io.vertx.core.http.WebSocketClientOptions;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.http.WebSocketVersion;
import io.vertx.core.net.JksOptions;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLHandshakeException;

public class WebsocketUtils {

  static void makeWsRequest(String request, Map<String, String> clientProps,
      TestKsqlRestApp restApp) throws SSLHandshakeException {
    makeWsRequest(request, clientProps, MultiMap.caseInsensitiveMultiMap(), restApp, true);
  }

  static void makeWsRequest(String request, Map<String, String> clientProps, MultiMap headers,
      TestKsqlRestApp restApp, boolean tls)
      throws SSLHandshakeException, UpgradeRejectedException {
    Vertx vertx = Vertx.vertx();
    WebSocketClient httpClient = null;
    try {

      WebSocketClientOptions httpClientOptions = new WebSocketClientOptions();
      if (tls) {
        httpClientOptions.setSsl(true).setVerifyHost(false);

        final Optional<JksOptions> trustStoreOptions =
            VertxSslOptionsFactory.getJksTrustStoreOptions(clientProps);

        final String alias = clientProps.get(KsqlClient.SSL_KEYSTORE_ALIAS_CONFIG);
        final Optional<JksOptions> keyStoreOptions =
            VertxSslOptionsFactory.buildJksKeyStoreOptions(clientProps, Optional.ofNullable(alias));

        trustStoreOptions.ifPresent(options -> httpClientOptions.setTrustOptions(options));
        keyStoreOptions.ifPresent(options -> httpClientOptions.setKeyCertOptions(options));
      }

      httpClient = vertx.createWebSocketClient(httpClientOptions);

      URI listener = tls ? restApp.getWssListener() : restApp.getWsListener();

      final URI uri = listener.resolve("/ws/query?request=" + request);

      CompletableFuture<Void> completableFuture = new CompletableFuture<>();

      httpClient
          .connect(new WebSocketConnectOptions()
              .setAbsoluteURI(uri.toString())
              .setHeaders(headers)
              .setVersion(WebSocketVersion.V07)).onComplete(ar -> {
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
