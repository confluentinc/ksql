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

import io.confluent.ksql.api.server.ApiServerConfig;
import io.confluent.ksql.api.server.Server;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.User;
import java.io.IOException;
import java.security.Principal;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.TextOutputCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.apache.commons.collections4.CollectionUtils;
import org.eclipse.jetty.jaas.callback.ObjectCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authentication provider that checks credentials specified in the JAAS config.
 */
public class JaasAuthProvider implements AuthProvider {

  private static final Logger log = LoggerFactory.getLogger(JaasAuthProvider.class);

  private final Server server;
  private final ApiServerConfig config;

  public JaasAuthProvider(final Server server, final ApiServerConfig config) {
    this.server = Objects.requireNonNull(server);
    this.config = Objects.requireNonNull(config);
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

    getUser(contextName, username, password, allowedRoles, userResult -> {
      if (userResult.succeeded()) {
        resultHandler.handle(Future.succeededFuture(userResult.result()));
      } else {
        resultHandler.handle(Future.failedFuture("invalid username/password"));
      }
    });
  }

  private void getUser(
      final String contextName,
      final String username,
      final String password,
      final List<String> allowedRoles,
      final Handler<AsyncResult<JaasUser>> handler
  ) {
    final LoginContext lc;
    try {
      lc = new LoginContext(contextName, new BasicCallbackHandler(username, password));
    } catch (LoginException | SecurityException e) {
      log.error("Failed to create LoginContext. " + e.getMessage());
      handler.handle(Future.failedFuture("Failed to create LoginContext."));
      return;
    }

    try {
      lc.login();
    } catch (LoginException le) {
      log.error("Failed to log in. " + le.getMessage());
      handler.handle(Future.failedFuture("Failed to log in: Invalid username/password."));
      return;
    }

    if (!validateRoles(lc, allowedRoles)) {
      log.error("Failed to log in: Invalid roles.");
      handler.handle(Future.failedFuture("Failed to log in: Invalid roles."));
      return;
    }

    final JaasUser result = new JaasUser(username, this);
    handler.handle(Future.succeededFuture(result));
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

  private static class BasicCallbackHandler implements CallbackHandler {

    private final String username;
    private final String password;

    BasicCallbackHandler(final String username, final String password) {
      this.username = Objects.requireNonNull(username, "username");
      this.password = Objects.requireNonNull(password, "password");
    }

    @Override
    public void handle(final Callback[] callbacks)
        throws IOException, UnsupportedCallbackException {
      for (final Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          final NameCallback nc = (NameCallback)callback;
          nc.setName(username);
        } else if (callback instanceof ObjectCallback) {
          final ObjectCallback oc = (ObjectCallback)callback;
          oc.setObject(password);
        } else if (callback instanceof PasswordCallback) {
          final PasswordCallback pc = (PasswordCallback)callback;
          pc.setPassword(password.toCharArray());
        } else if (callback instanceof TextOutputCallback) {
          final TextOutputCallback toc = (TextOutputCallback) callback;
          switch (toc.getMessageType()) {
            case TextOutputCallback.ERROR:
              log.error(toc.getMessage());
              break;
            case TextOutputCallback.WARNING:
              log.warn(toc.getMessage());
              break;
            case TextOutputCallback.INFORMATION:
              log.info(toc.getMessage());
              break;
            default:
              throw new IOException("Unsupported message type: " + toc.getMessageType());
          }
        } else {
          throw new UnsupportedCallbackException(callback);
        }
      }
    }
  }
}
