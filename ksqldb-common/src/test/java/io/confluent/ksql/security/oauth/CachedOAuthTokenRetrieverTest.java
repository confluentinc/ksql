/*
 * Copyright 2024 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.security.oauth;


import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;

import io.confluent.ksql.security.oauth.exceptions.KsqlOAuthTokenRetrieverException;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenValidator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.BasicOAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ValidateException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class CachedOAuthTokenRetrieverTest {

  @Mock
  AccessTokenRetriever accessTokenRetriever;

  @Mock
  AccessTokenValidator accessTokenValidator;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  OAuthTokenCache oAuthTokenCache;

  @InjectMocks
  CachedOAuthTokenRetriever cachedOAuthTokenRetriever;

  private final String tokenString1 = "token1";
  private OAuthBearerToken token1;

  @Test
  public void TestGetTokenWithValidCache() throws IOException {

    //Token1 has validity of 100 ms
    token1 = new BasicOAuthBearerToken(tokenString1,
        Collections.emptySet(),
        2 * 1000L,
        "random",
        0L);

    when(accessTokenRetriever.retrieve()).thenReturn(tokenString1);
    when(accessTokenValidator.validate(tokenString1)).thenReturn(token1);
    when(oAuthTokenCache.isTokenExpired()).thenReturn(true).thenReturn(false);
    Mockito.doNothing().when(oAuthTokenCache).setCurrentToken(Mockito.any(OAuthBearerToken.class));
    when(oAuthTokenCache.getCurrentToken().value()).thenReturn(tokenString1);

    Assert.assertEquals(tokenString1, cachedOAuthTokenRetriever.getToken());

    //Expects second call to retrieve token to get the cached token1 instead of
    // making a second network call to get a new token
    Assert.assertEquals(tokenString1, cachedOAuthTokenRetriever.getToken());
    Mockito.verify(accessTokenValidator, times(1)).validate(anyString());
    Mockito.verify(accessTokenRetriever, times(1)).retrieve();
    Mockito.verify(oAuthTokenCache, times(2)).isTokenExpired();
  }

  @Test
  public void TestGetTokenWithExpiredCache() throws IOException {
    //Token1 has validity of 0 ms
    token1 = new BasicOAuthBearerToken(tokenString1,
        Collections.emptySet(),
        100,
        "random",
        0L);
    String tokenString2 = "token2";
    OAuthBearerToken token2 = new BasicOAuthBearerToken(tokenString2,
        Collections.emptySet(),
        100,
        "random",
        0L);

    //chaining the return values to return tokenString1 first and then tokenString2 on the
    // subsequent call
    when(accessTokenRetriever.retrieve()).thenReturn(tokenString1).thenReturn(tokenString2);
    when(accessTokenValidator.validate(tokenString1)).thenReturn(token1);
    when(accessTokenValidator.validate(tokenString2)).thenReturn(token2);
    // return true both the times
    when(oAuthTokenCache.isTokenExpired()).thenReturn(true);
    Mockito.doNothing().when(oAuthTokenCache).setCurrentToken(Mockito.any(OAuthBearerToken.class));
    when(oAuthTokenCache.getCurrentToken().value()).thenReturn(tokenString1)
        .thenReturn(tokenString2);

    Assert.assertEquals(tokenString1, cachedOAuthTokenRetriever.getToken());
    Assert.assertEquals(tokenString2, cachedOAuthTokenRetriever.getToken());
    Mockito.verify(accessTokenValidator, times(2)).validate(anyString());
    Mockito.verify(accessTokenRetriever, times(2)).retrieve();
    Mockito.verify(oAuthTokenCache, times(2)).isTokenExpired();
  }

  @Test
  public void TestGetTokenThrowsException() throws IOException {
    String ioError = "Returned 401";
    when(oAuthTokenCache.isTokenExpired()).thenReturn(true);
    // Test whether IO exception is handled first when token retrieval,
    // then test whether Validation exception is handled when token validation
    when(accessTokenRetriever.retrieve()).thenThrow(new IOException(ioError))
        .thenReturn(tokenString1);

    Assert.assertThrows(ioError, KsqlOAuthTokenRetrieverException.class, () -> {
      cachedOAuthTokenRetriever.getToken();
    });

    String validationError = "Malformed JWT provided";
    when(accessTokenValidator.validate(tokenString1)).thenThrow(
        new ValidateException(validationError));
    Assert.assertThrows(validationError, KsqlOAuthTokenRetrieverException.class, () -> {
      cachedOAuthTokenRetriever.getToken();
    });

  }
}