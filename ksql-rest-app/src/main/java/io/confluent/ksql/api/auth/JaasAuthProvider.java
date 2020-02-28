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
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.User;
import java.io.IOException;
import java.util.Objects;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.TextOutputCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
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

    server.getWorkerExecutor().executeBlocking(
        p -> getUser(contextName, username, password, p),
        resultHandler
    );
  }

  private void getUser(
      final String contextName,
      final String username,
      final String password,
      final Promise<User> promise
  ) {
    final LoginContext lc;
    try {
      lc = new LoginContext(contextName, new BasicCallbackHandler(username, password));
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

    promise.complete(new JaasUser(username, this));
  }

  private void checkUserPermission(
      final String username,
      final Handler<AsyncResult<Boolean>> resultHandler
  ) {
    // no authorization yet; authenticated users have all permissions
    resultHandler.handle(Future.succeededFuture(true));
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
