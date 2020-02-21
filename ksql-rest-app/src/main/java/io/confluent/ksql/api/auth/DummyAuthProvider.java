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
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.User;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dummy auth provider for testing purposes. Will replace with JAAS.
 */
public class DummyAuthProvider implements AuthProvider {

  private static final Logger log = LoggerFactory.getLogger(DummyAuthProvider.class);

  private final Vertx vertx;
  private final ApiServerConfig config;

  public DummyAuthProvider(final Vertx vertx, final ApiServerConfig config) {
    this.vertx = Objects.requireNonNull(vertx);
    this.config = Objects.requireNonNull(config);
  }

  @Override
  public void authenticate(JsonObject authInfo, Handler<AsyncResult<User>> resultHandler) {
    String username = authInfo.getString("username");
    if (username == null) {
      resultHandler.handle(Future.failedFuture("authInfo missing 'username' field"));
      return;
    }
    String password = authInfo.getString("password");
    if (password == null) {
      resultHandler.handle(Future.failedFuture("authInfo missing 'password' field"));
      return;
    }
    getUser(username, userResult -> {
      if (userResult.succeeded()) {
        DummyUser user = userResult.result();
        if (user.checkPassword(password)) {
          resultHandler.handle(Future.succeededFuture(user));
        } else {
          resultHandler.handle(Future.failedFuture("invalid username/password"));
        }
      } else {
        resultHandler.handle(Future.failedFuture("invalid username/password"));
      }
    });
  }

  private void getUser(String username, Handler<AsyncResult<DummyUser>> handler) {
    // TODO: look up user, probably in an async way
    DummyUser result = new DummyUser(username, "password", this);
    handler.handle(Future.succeededFuture(result));
  }

  private void checkUserPermission(String username, Handler<AsyncResult<Boolean>> resultHandler) {
    resultHandler.handle(Future.succeededFuture(true));
  }

  @SuppressWarnings("deprecation") // TODO: what replaced AbstractUser given deprecation?
  static class DummyUser extends io.vertx.ext.auth.AbstractUser {

    private final String username;
    private final String password;
    private DummyAuthProvider authProvider;
    private JsonObject principal;

    DummyUser(final String username, final String password, final DummyAuthProvider authProvider) {
      this.username = username;
      this.password = password;
      this.authProvider = authProvider;
    }

    boolean checkPassword(final String password) {
      return Objects.equals(this.password, password);
    }

    @Override
    public void doIsPermitted(String permission, Handler<AsyncResult<Boolean>> resultHandler) {
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
    public void setAuthProvider(AuthProvider authProvider) {
      if (authProvider instanceof DummyAuthProvider) {
        this.authProvider = (DummyAuthProvider)authProvider;
      } else {
        throw new IllegalArgumentException("Not a DummyAuthProvider");
      }
    }
  }
}
