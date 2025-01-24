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
import io.confluent.ksql.security.KsqlPrincipal;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.Authorizations;
import io.vertx.ext.auth.authorization.RoleBasedAuthorization;
import io.vertx.ext.auth.authorization.impl.AuthorizationsImpl;
import java.util.Objects;
import java.util.Optional;
import org.eclipse.jetty.jaas.JAASLoginService;
import org.eclipse.jetty.server.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authentication provider that checks credentials specified in the JAAS config.
 */
public class JaasAuthProvider implements AuthProvider {

  private static final Logger LOG = LoggerFactory.getLogger(JaasAuthProvider.class);

  private final Server server;
  private final String contextName;
  private final LoginContextSupplier loginContext;

  @VisibleForTesting
  @FunctionalInterface
  interface LoginContextSupplier {
    JAASLoginService get();
  }

  public JaasAuthProvider(
          final Server server,
          final String contextName
  ) {
    this(server, contextName, JAASLoginService::new);
  }

  @VisibleForTesting
  JaasAuthProvider(
          final Server server,
          final String contextName,
          final LoginContextSupplier loginContextSupplier
  ) {
    this.server = Objects.requireNonNull(server, "server");
    this.contextName = Objects.requireNonNull(contextName, "contextName");
    this.loginContext = Objects.requireNonNull(loginContextSupplier, "loginContextSupplier");
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
            promisedUser -> getUser(contextName, username, password, promisedUser),
            false,
            resultHandler
    );
  }

  private void getUser(
          final String contextName,
          final String username,
          final String password,
          final Promise<User> promisedUser
  ) {
    final JAASLoginService login = loginContext.get();
    login.setCallbackHandlerClass(BasicCallbackHandler.class.getName());
    login.setLoginModuleName(contextName);

    try {
      login.start();
    } catch (final Exception e) {
      LOG.error("Could not start login service.", e);
      promisedUser.fail("Could not start login service.");
    }

    final UserIdentity user = login.login(username, password, null);

    if (user == null) {
      LOG.error("Failed to log in. ");
      promisedUser.fail("Failed to log in: Invalid username/password.");
      return;
    }

    // if the subject from the login context is already a KsqlPrincipal, use the subject
    // directly rather than creating a new one
    final Optional<KsqlPrincipal> ksqlPrincipal = user.getSubject().getPrincipals().stream()
            .filter(KsqlPrincipal.class::isInstance)
            .map(KsqlPrincipal.class::cast)
            .findFirst();

    final Authorizations authorizations = new AuthorizationsImpl();
    user.getSubject()
            .getPrincipals()
            .forEach(p -> authorizations.add("default",
                    RoleBasedAuthorization.create(p.getName())));

    promisedUser.complete(new ApiUser() {

      @Override
      public Authorizations authorizations() {
        return authorizations;
      }

      @Override
      public JsonObject attributes() {
        throw new UnsupportedOperationException();
      }

      @Override
      public User isAuthorized(
              final Authorization authority,
              final Handler<AsyncResult<Boolean>> resultHandler
      ) {
        throw new UnsupportedOperationException();
      }

      @Override
      public JsonObject principal() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void setAuthProvider(final AuthProvider authProvider) {
        throw new UnsupportedOperationException();
      }

      @Override
      public KsqlPrincipal getPrincipal() {
        return ksqlPrincipal
                .orElseGet(() -> new JaasPrincipal(username, password));
      }

      @Override
      public User merge(final User other) {
        throw new UnsupportedOperationException();
      }
    });
  }
}
