/*
 * Copyright 2021 Confluent Inc.
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

import java.io.IOException;
import java.util.Objects;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.TextOutputCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import org.eclipse.jetty.jaas.callback.ObjectCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BasicCallbackHandler implements CallbackHandler {

  private static final Logger log = LoggerFactory.getLogger(BasicCallbackHandler.class);

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
        final NameCallback nc = (NameCallback) callback;
        nc.setName(username);
      } else if (callback instanceof ObjectCallback) {
        final ObjectCallback oc = (ObjectCallback)callback;
        oc.setObject(password);
      } else if (callback instanceof PasswordCallback) {
        final PasswordCallback pc = (PasswordCallback) callback;
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
        // We ignore unknown callback types - e.g. Jetty implementation might pass us Jetty specific
        // stuff which we can't deal with
      }
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final BasicCallbackHandler that = (BasicCallbackHandler) o;
    return username.equals(that.username)
        && password.equals(that.password);
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, password);
  }
}