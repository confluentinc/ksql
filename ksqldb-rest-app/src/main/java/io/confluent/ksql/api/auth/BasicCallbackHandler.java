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

import java.io.IOException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.TextOutputCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.security.jaas.callback.DefaultCallbackHandler;
import org.eclipse.jetty.security.jaas.callback.ObjectCallback;

public class BasicCallbackHandler extends DefaultCallbackHandler {

  private static final Logger log = LogManager.getLogger(BasicCallbackHandler.class);

  @Override
  public void handle(final Callback[] callbacks)
      throws IOException, UnsupportedCallbackException {
    for (final Callback callback : callbacks) {
      if (callback instanceof NameCallback) {
        final NameCallback nc = (NameCallback) callback;
        nc.setName(getUserName());
      } else if (callback instanceof ObjectCallback) {
        final ObjectCallback oc = (ObjectCallback)callback;
        oc.setObject(getCredential());
      } else if (callback instanceof PasswordCallback) {
        final PasswordCallback pc = (PasswordCallback) callback;
        pc.setPassword(((String) getCredential()).toCharArray());
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
}