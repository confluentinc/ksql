/*
 * Copyright 2024 Confluent Inc.
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

package io.confluent.ksql.security.oauth;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.security.oauth.exceptions.KsqlOAuthTokenRetrieverException;
import java.io.IOException;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenValidator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ValidateException;

public class CachedOAuthTokenRetriever {
  private final AccessTokenRetriever accessTokenRetriever;
  private final AccessTokenValidator accessTokenValidator;
  private final OAuthTokenCache authTokenCache;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public CachedOAuthTokenRetriever(final AccessTokenRetriever accessTokenRetriever,
                                   final AccessTokenValidator accessTokenValidator,
                                   final OAuthTokenCache authTokenCache) {
    this.accessTokenRetriever = accessTokenRetriever;
    this.accessTokenValidator = accessTokenValidator;
    this.authTokenCache = authTokenCache;
  }

  public String getToken() {
    if (authTokenCache.isTokenExpired()) {
      String token = null;
      try {
        token = accessTokenRetriever.retrieve();
      } catch (IOException | RuntimeException e) {
        throw new KsqlOAuthTokenRetrieverException(
            "Failed to Retrieve OAuth Token for KSQL", e);
      }

      final OAuthBearerToken oauthBearerToken;
      try {
        oauthBearerToken = accessTokenValidator.validate(token);
      } catch (ValidateException e) {
        throw new KsqlOAuthTokenRetrieverException(
            "OAuth Token for KSQL is Invalid", e);
      }

      authTokenCache.setCurrentToken(oauthBearerToken);
    }
    return authTokenCache.getCurrentToken().value();
  }
}
