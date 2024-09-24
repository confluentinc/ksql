/*
 * Copyright 2024 Confluent Inc.
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

package io.confluent.ksql.api.client;

import io.confluent.ksql.api.AuthTest;
import io.confluent.ksql.api.auth.AuthenticationPlugin;
import io.confluent.ksql.api.client.util.IdentityProviderService;
import io.confluent.ksql.api.server.KsqlApiException;
import io.confluent.ksql.api.server.Server;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.security.KsqlDefaultSecurityExtension;
import io.confluent.ksql.security.oauth.IdpConfig;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.web.RoutingContext;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.confluent.ksql.rest.Errors.ERROR_CODE_UNAUTHORIZED;

public class ClientOAuthTest extends ClientTest {
  private static final String APP1_DEVELOPER = "app1-developer";
  private final AuthenticationPlugin authenticationPlugin = new OAuthPlugin();
  private static final IdentityProviderService idp = new IdentityProviderService();

  @Override
  public void setUp() {
    idp.start();
    super.setUp();
  }

  @Override
  protected ClientOptions createJavaClientOptions() {
    return super.createJavaClientOptions()
        .setIdpConfig(new IdpConfig.Builder()
            .withTokenEndpointUrl(idp.getTokenEndpoint())
            .withClientId(APP1_DEVELOPER)
            .withClientSecret(APP1_DEVELOPER)
            .build());
  }

  @Override
  protected void createServer(KsqlRestConfig serverConfig) {
    server = new Server(vertx, serverConfig, testEndpoints,
        new KsqlDefaultSecurityExtension(), Optional.of(authenticationPlugin),
        serverState, Optional.empty());

    try {
      server.start();
    } catch (final Exception e) {
      server = null;
      throw e;
    }
  }

  public static class OAuthPlugin implements AuthenticationPlugin {
    @Override
    public void configure(Map<String, ?> map) {
    }

    @Override
    public CompletableFuture<Principal> handleAuth(RoutingContext routingContext,
                                                   WorkerExecutor workerExecutor) {
      // Simulating a server side login and returning a principal completable future

      final VertxCompletableFuture<Principal> vcf = new VertxCompletableFuture<>();

      // get sub-claim out of the jwt. Since, this is a unit test,
      // let's not worry about signature stuff.
      String authHeader = getAuthHeader(routingContext);
      String jwt = authHeader.substring("Bearer ".length());
      String[] parts = jwt.split("\\.");
      String payload = new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8);
      JSONObject jsonPayload = new JSONObject(payload);
      String sub = jsonPayload.getString("sub");

      workerExecutor.executeBlocking(promise -> {
        if (Objects.equals(sub, APP1_DEVELOPER)) {
          promise.complete(new AuthTest.StringPrincipal(APP1_DEVELOPER));
        } else {
          final KsqlApiException e = new KsqlApiException(
              "Unauthorized", ERROR_CODE_UNAUTHORIZED);

          routingContext.fail(401, e);
          promise.fail(e);
        }
      }, vcf);

      return vcf;
    }
  }
}
