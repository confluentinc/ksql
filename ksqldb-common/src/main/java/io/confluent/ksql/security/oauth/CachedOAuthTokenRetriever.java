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
import org.apache.kafka.common.security.oauthbearer.JwtRetriever;
import org.apache.kafka.common.security.oauthbearer.JwtRetrieverException;
import org.apache.kafka.common.security.oauthbearer.JwtValidator;
import org.apache.kafka.common.security.oauthbearer.JwtValidatorException;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

public class CachedOAuthTokenRetriever {
  private final JwtRetriever accessTokenRetriever;
  private final JwtValidator accessTokenValidator;
  private final OAuthTokenCache authTokenCache;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public CachedOAuthTokenRetriever(final JwtRetriever accessTokenRetriever,
                                   final JwtValidator accessTokenValidator,
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
      } catch (JwtRetrieverException e) {
        throw new KsqlOAuthTokenRetrieverException(
            "Failed to Retrieve OAuth Token for KSQL", e);
      }

      final OAuthBearerToken oauthBearerToken;
      try {
        oauthBearerToken = accessTokenValidator.validate(token);
      } catch (JwtValidatorException e) {
        throw new KsqlOAuthTokenRetrieverException(
            "OAuth Token for KSQL is Invalid", e);
      }

      authTokenCache.setCurrentToken(oauthBearerToken);
    }
    return authTokenCache.getCurrentToken().value();
  }
}
