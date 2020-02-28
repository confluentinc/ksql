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

import static org.mockito.Mockito.verify;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import org.eclipse.jetty.jaas.callback.ObjectCallback;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BasicCallbackHandlerTest {

  private static final String USERNAME = "foo";
  private static final String PASSWORD = "secret";

  @Mock
  private NameCallback nameCallback;
  @Mock
  private ObjectCallback objectCallback;
  @Mock
  private PasswordCallback passwordCallback;
  @Mock
  private Callback unknownCallback;

  private BasicCallbackHandler callbackHandler;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() {
    callbackHandler = new BasicCallbackHandler(USERNAME, PASSWORD);
  }

  @Test
  public void shouldHandleCallbacks() throws Exception {
    // When:
    callbackHandler.handle(new Callback[]{nameCallback, objectCallback, passwordCallback});

    // Then:
    verify(nameCallback).setName(USERNAME);
    verify(objectCallback).setObject(PASSWORD);
    verify(passwordCallback).setPassword(PASSWORD.toCharArray());
  }

  @Test
  public void shouldThrowOnUnsupportedCallback() throws Exception {
    // Expect:
    expectedException.expect(UnsupportedCallbackException.class);

    // When:
    callbackHandler.handle(new Callback[]{unknownCallback});
  }
}
