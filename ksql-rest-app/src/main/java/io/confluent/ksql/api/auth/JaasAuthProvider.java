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

package io.confluent.ksql.api.auth;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.api.server.Server;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.User;
import java.security.Principal;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authentication provider that checks credentials specified in the JAAS config.
 */
public class JaasAuthProvider implements AuthProvider {

  private static final Logger log = LoggerFactory.getLogger(JaasAuthProvider.class);

  private final Server server;
  private final ApiServerConfig config;
  private final LoginContextSupplier loginContextSupplier;

  public JaasAuthProvider(final Server server, final ApiServerConfig config) {
    this(server, config, LoginContext::new);
  }

  @VisibleForTesting
  JaasAuthProvider(
      final Server server,
      final ApiServerConfig config,
      final LoginContextSupplier loginContextSupplier
  ) {
    this.server = Objects.requireNonNull(server, "server");
    this.config = Objects.requireNonNull(config, "config");
    this.loginContextSupplier =
        Objects.requireNonNull(loginContextSupplier, "loginContextSupplier");
  }

  @VisibleForTesting
  @FunctionalInterface
  interface LoginContextSupplier {
    LoginContext get(String name, CallbackHandler callbackHandler) throws LoginException;
  }

  @Override
  public void authenticate(
      final JsonObject authInfo,
      final Handler<AsyncResult<User>> resultHandler
  ) {
    final String username = authInfo.getString("username");
    if (username == null) {
      resultHandler.handle(Future.failedFuture("authInfo missing 'username' field"));
      return;
    }
    final String password = authInfo.getString("password");
    if (password == null) {
      resultHandler.handle(Future.failedFuture("authInfo missing 'password' field"));
      return;
    }

    final String contextName = config.getString(ApiServerConfig.AUTHENTICATION_REALM_CONFIG);
    final List<String> allowedRoles = config.getList(ApiServerConfig.AUTHENTICATION_ROLES_CONFIG);

    server.getWorkerExecutor().executeBlocking(
        p -> getUser(contextName, username, password, allowedRoles, p),
        resultHandler
    );
  }

  private void getUser(
      final String contextName,
      final String username,
      final String password,
      final List<String> allowedRoles,
      final Promise<User> promise
  ) {
    final LoginContext lc;
    try {
      lc = loginContextSupplier.get(contextName, new BasicCallbackHandler(username, password));
    } catch (LoginException | SecurityException e) {
      log.error("Failed to create LoginContext. " + e.getMessage());
      promise.fail("Failed to create LoginContext.");
      return;
    }

    try {
      lc.login();
    } catch (LoginException le) {
      log.error("Failed to log in. " + le.getMessage());
      promise.fail("Failed to log in: Invalid username/password.");
      return;
    }

    if (!validateRoles(lc, allowedRoles)) {
      log.error("Failed to log in: Invalid roles.");
      promise.fail("Failed to log in: Invalid roles.");
      return;
    }

    promise.complete(new JaasUser(username, this));
  }

  private void checkUserPermission(
      final String username,
      final Handler<AsyncResult<Boolean>> resultHandler
  ) {
    // no authorization yet (besides JAAS role check during login)
    // consequently, authenticated users have all permissions
    resultHandler.handle(Future.succeededFuture(true));
  }

  private static boolean validateRoles(final LoginContext lc, final List<String> allowedRoles) {
    if (allowedRoles.contains("*")) {
      // all users allowed
      return true;
    }

    final Set<String> userRoles = lc.getSubject().getPrincipals().stream()
        .map(Principal::getName)
        .collect(Collectors.toSet());
    return !CollectionUtils.intersection(userRoles, allowedRoles).isEmpty();
  }

  @SuppressWarnings("deprecation")
  static class JaasUser extends io.vertx.ext.auth.AbstractUser {

    private final String username;
    private JaasAuthProvider authProvider;
    private JsonObject principal;

    JaasUser(final String username, final JaasAuthProvider authProvider) {
      this.username = Objects.requireNonNull(username, "username");
      this.authProvider = Objects.requireNonNull(authProvider, "authProvider");
    }

    @Override
    public void doIsPermitted(
        final String permission,
        final Handler<AsyncResult<Boolean>> resultHandler
    ) {
      authProvider.checkUserPermission(username, resultHandler);
    }

    @Override
    public JsonObject principal() {
      if (principal == null) {
        principal = new JsonObject().put("username", username);
      }
      return principal;
    }

    @Override
    public void setAuthProvider(final AuthProvider authProvider) {
      if (authProvider instanceof JaasAuthProvider) {
        this.authProvider = (JaasAuthProvider)authProvider;
      } else {
        throw new IllegalArgumentException("Not a JaasAuthProvider");
      }
    }
  }
}
