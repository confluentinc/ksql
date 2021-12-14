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
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.security.KsqlPrincipal;
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
import java.util.Optional;
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
  private final KsqlRestConfig config;
  private final LoginContextSupplier loginContextSupplier;
  private final List<String> allowedRoles;
  private final String contextName;

  public JaasAuthProvider(final Server server, final KsqlRestConfig config) {
    this(server, config, LoginContext::new);
  }

  @VisibleForTesting
  JaasAuthProvider(
      final Server server,
      final KsqlRestConfig config,
      final LoginContextSupplier loginContextSupplier
  ) {
    this.server = Objects.requireNonNull(server, "server");
    this.config = Objects.requireNonNull(config, "config");
    this.loginContextSupplier =
        Objects.requireNonNull(loginContextSupplier, "loginContextSupplier");
    final List<String> authRoles = config.getList(KsqlRestConfig.AUTHENTICATION_ROLES_CONFIG);
    this.allowedRoles = authRoles.stream()
        .filter(role -> !"*".equals(role)) // remove "*"
        .map(role -> "**".equals(role) ? "*" : role) // Change "**" to "*"
        .collect(Collectors.toList());
    this.contextName = config.getString(KsqlRestConfig.AUTHENTICATION_REALM_CONFIG);
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

    server.getWorkerExecutor().executeBlocking(
        p -> getUser(contextName, username, password, allowedRoles, p),
        false,
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

    // We do the actual authorization here not in the User class
    final boolean authorized = validateRoles(lc, allowedRoles);

    // if the subject from the login context is already a KsqlPrincipal, use the subject
    // directly rather than creating a new one
    final Optional<KsqlPrincipal> ksqlPrincipal = lc.getSubject().getPrincipals().stream()
        .filter(p -> p instanceof KsqlPrincipal)
        .map(p -> (KsqlPrincipal)p)
        .findFirst();
    final JaasUser user = ksqlPrincipal.isPresent()
        ? new JaasUser(ksqlPrincipal.get(), authorized)
        : new JaasUser(username, password, authorized);
    promise.complete(user);
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
  static class JaasUser extends io.vertx.ext.auth.AbstractUser implements ApiUser {

    private final KsqlPrincipal principal;
    private final boolean authorized;

    JaasUser(
        final String username,
        final String password,
        final boolean authorized
    ) {
      this(
          new JaasPrincipal(
              Objects.requireNonNull(username, "username"),
              Objects.requireNonNull(password, "password")),
          authorized);
    }

    JaasUser(
        final KsqlPrincipal principal,
        final boolean authorized
    ) {
      this.principal = Objects.requireNonNull(principal, "principal");
      this.authorized = authorized;
    }

    @Override
    public void doIsPermitted(
        final String permission,
        final Handler<AsyncResult<Boolean>> resultHandler
    ) {
      resultHandler.handle(Future.succeededFuture(authorized));
    }

    @Override
    public JsonObject principal() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setAuthProvider(final AuthProvider authProvider) {
    }

    @Override
    public KsqlPrincipal getPrincipal() {
      return principal;
    }
  }
}
