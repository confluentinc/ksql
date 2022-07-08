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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.api.auth.JaasAuthProvider.JaasUser;
import io.confluent.ksql.api.auth.JaasAuthProvider.LoginContextSupplier;
import io.confluent.ksql.api.server.Server;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import java.security.Principal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;
import javax.security.auth.Subject;
import org.eclipse.jetty.jaas.JAASLoginService;
import org.eclipse.jetty.server.UserIdentity;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JaasAuthProviderTest {

  private static final String REALM = "realm";
  private static final String USERNAME = "foo";
  private static final String PASSWORD = "secret";

  @Mock
  private Server server;
  @Mock
  private WorkerExecutor worker;
  @Mock
  private KsqlRestConfig config;
  @Mock
  private JsonObject authInfo;
  @Mock
  private Handler<AsyncResult<User>> userHandler;
  @Captor
  private ArgumentCaptor<AsyncResult<User>> userCaptor;
  @Mock
  private LoginContextSupplier loginContextSupplier;
  @Mock
  private JAASLoginService loginService;
  @Mock
  private UserIdentity userIdentity;
  @Mock
  private Subject subject;

  private JaasAuthProvider authProvider;

  @Before
  public void setUp() throws Exception {
    final BasicCallbackHandler callbackHandler = new BasicCallbackHandler();
    callbackHandler.setUserName(USERNAME);
    callbackHandler.setCredential(PASSWORD);

    handleAsyncExecution();
    when(config.getString(KsqlRestConfig.AUTHENTICATION_REALM_CONFIG)).thenReturn(REALM);
    when(authInfo.getString("username")).thenReturn(USERNAME);
    when(authInfo.getString("password")).thenReturn(PASSWORD);
    when(loginContextSupplier.get()).thenReturn(loginService);
    when(loginService.login(eq(USERNAME), eq(PASSWORD), any())).thenReturn(userIdentity);
    when(userIdentity.getSubject()).thenReturn(subject);

    authProvider = new JaasAuthProvider(server, config, loginContextSupplier);
  }

  @Test
  public void shouldAuthenticateWithWildcardAllowedRole() throws Exception {
    // Given:
    givenAllowedRoles("**");
    givenUserRoles();

    // When:
    authProvider.authenticate(authInfo, userHandler);

    // Then:
    verifyAuthorizedSuccessfulLogin();
  }

  @Test
  public void shouldAuthenticateWithNonWildcardRole() throws Exception {
    // Given:
    givenAllowedRoles("user");
    givenUserRoles("user");

    // When:
    authProvider.authenticate(authInfo, userHandler);

    // Then:
    verifyAuthorizedSuccessfulLogin();
  }

  @Test
  public void shouldAuthenticateWithAdditionalAllowedRoles() throws Exception {
    // Given:
    givenAllowedRoles("user", "other");
    givenUserRoles("user");

    // When:
    authProvider.authenticate(authInfo, userHandler);

    // Then:
    verifyAuthorizedSuccessfulLogin();
  }

  @Test
  public void shouldAuthenticateWithExtraRoles() throws Exception {
    // Given:
    givenAllowedRoles("user");
    givenUserRoles("user", "other");

    // When:
    authProvider.authenticate(authInfo, userHandler);

    // Then:
    verifyAuthorizedSuccessfulLogin();
  }

  @Test
  public void shouldFailToAuthenticateOnMissingUsername() {
    // Given:
    when(authInfo.getString("username")).thenReturn(null);

    // When:
    authProvider.authenticate(authInfo, userHandler);

    // Then:
    verifyLoginFailure("authInfo missing 'username' field");
  }

  @Test
  public void shouldFailToAuthenticateOnMissingPassword() {
    // Given:
    when(authInfo.getString("password")).thenReturn(null);

    // When:
    authProvider.authenticate(authInfo, userHandler);

    // Then:
    verifyLoginFailure("authInfo missing 'password' field");
  }

  @Test
  public void shouldFailToAuthenticateWithNoRole() throws Exception {
    // Given:
    givenAllowedRoles("user");
    givenUserRoles();

    // When:
    authProvider.authenticate(authInfo, userHandler);

    // Then:
    verifyUnauthorizedSuccessfulLogin();
  }

  @Test
  public void shouldFailToAuthenticateWithNonAllowedRole() throws Exception {
    // Given:
    givenAllowedRoles("user");
    givenUserRoles("other");

    // When:
    authProvider.authenticate(authInfo, userHandler);

    // Then:
    verifyUnauthorizedSuccessfulLogin();
  }

  private void givenAllowedRoles(final String... roles) {
    when(config.getList(KsqlRestConfig.AUTHENTICATION_ROLES_CONFIG))
        .thenReturn(Arrays.asList(roles));

    authProvider = new JaasAuthProvider(server, config, loginContextSupplier);
  }

  private void givenUserRoles(final String... roles) {
    final Set<Principal> principals = new HashSet<>();
    principals.add(principalWithName(USERNAME));
    Stream.of(roles).forEach(r -> principals.add(principalWithName(r)));

    when(subject.getPrincipals()).thenReturn(principals);
  }

  private void verifyAuthorizedSuccessfulLogin() throws Exception {
    verifyLoginSuccessWithAuthorization(true);
  }

  private void verifyUnauthorizedSuccessfulLogin() throws Exception {
    verifyLoginSuccessWithAuthorization(false);
  }

  private void verifyLoginSuccessWithAuthorization(final boolean isAuthorized) throws Exception {
    verify(userHandler).handle(userCaptor.capture());
    final AsyncResult<User> result = userCaptor.getValue();
    assertThat(result.succeeded(), is(true));

    assertThat(result.result(), instanceOf(JaasUser.class));
    final JaasUser user = (JaasUser) result.result();

    assertThat(user.getPrincipal(), instanceOf(JaasPrincipal.class));
    final JaasPrincipal apiPrincipal = (JaasPrincipal) user.getPrincipal();
    assertThat(apiPrincipal.getName(), is(USERNAME));

    final CountDownLatch latch = new CountDownLatch(1);
    user.doIsPermitted("some permission", ar -> {
      assertThat(ar.result(), is(isAuthorized));
      latch.countDown();
    });
    latch.await();
  }

  private void verifyLoginFailure(final String expectedMsg) {
    verify(userHandler).handle(userCaptor.capture());
    final AsyncResult<User> result = userCaptor.getValue();
    assertThat(result.succeeded(), is(false));
    assertThat(result.cause().getMessage(), is(expectedMsg));
  }

  private static Principal principalWithName(final String name) {
    final Principal mockPrincipal = mock(Principal.class);
    when(mockPrincipal.getName()).thenReturn(name);
    return mockPrincipal;
  }

  private void handleAsyncExecution() {
    when(server.getWorkerExecutor()).thenReturn(worker);
    doAnswer(invocation -> {
      final Handler<Promise<User>> blockingCodeHandler = invocation.getArgument(0);
      final Handler<AsyncResult<User>> resultHandler = invocation.getArgument(2);
      final Promise<User> promise = Promise.promise();
      promise.future().onComplete(resultHandler);
      blockingCodeHandler.handle(promise);
      return null;
    }).when(worker).executeBlocking(any(), eq(false), any());
  }
}
