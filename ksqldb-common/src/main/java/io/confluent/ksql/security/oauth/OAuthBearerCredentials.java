/*
 * Copyright 2019 Confluent Inc.
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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.security.Credentials;
import io.confluent.ksql.security.KsqlClientConfig;
import java.net.URL;
import java.util.Map;
import javax.net.ssl.SSLSocketFactory;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenValidator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpAccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JaasOptionsUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.LoginAccessTokenValidator;

public class OAuthBearerCredentials implements Credentials {

  private CachedOAuthTokenRetriever tokenRetriever;

  @Override
  public void configure(final Map<String, ?> configs) {
    validateConfigs(configs);
    final ConfigurationUtils configUtils = new ConfigurationUtils(configs);
    final CachedOAuthTokenRetriever cachedOAuthTokenRetriever = new CachedOAuthTokenRetriever(
        getAccessTokenRetriever(configUtils, configs),
        getAccessTokenValidator(configs),
        getOAuthTokenCache(configs));
    init(cachedOAuthTokenRetriever);
  }

  // This should only be used for testing purposes
  @VisibleForTesting
  void init(final CachedOAuthTokenRetriever tokenRetriever) {
    this.tokenRetriever = tokenRetriever;
  }

  @Override
  public String getAuthHeader() {
    return "Bearer " + retrieveToken();
  }

  @Override
  public void validateConfigs(final Map<String, ?> configs) throws ConfigException {

    final String tokenEndpointUrl = (String) configs
        .get(KsqlClientConfig.BEARER_AUTH_TOKEN_ENDPOINT_URL);
    final String clientId = (String) configs.get(KsqlClientConfig.BEARER_AUTH_CLIENT_ID);
    final String clientSecret = (String) configs.get(KsqlClientConfig.BEARER_AUTH_CLIENT_SECRET);
    if ((tokenEndpointUrl == null || tokenEndpointUrl.isEmpty())) {
      throw new ConfigException("Cannot configure OAuthBearerCredentials without "
          + "proper tokenEndpointUrl.");
    }
    if ((clientId == null || clientId.isEmpty())) {
      throw new ConfigException("Cannot configure OAuthBearerCredentials without "
          + "proper clientId.");
    }
    if ((clientSecret == null || clientSecret.isEmpty())) {
      throw new ConfigException("Cannot configure OAuthBearerCredentials without "
          + "proper clientSecret.");
    }
  }

  public String retrieveToken() {
    return tokenRetriever.getToken();
  }

  private AccessTokenRetriever getAccessTokenRetriever(final ConfigurationUtils configUtils,
                                                       final Map<String, ?> configs) {
    final String clientId = configUtils.validateString(KsqlClientConfig.BEARER_AUTH_CLIENT_ID);
    final String clientSecret = configUtils.validateString(
        KsqlClientConfig.BEARER_AUTH_CLIENT_SECRET);
    final String scope = configUtils.validateString(KsqlClientConfig.BEARER_AUTH_SCOPE, false);

    //Keeping following configs needed by HttpAccessTokenRetriever as constants and not exposed to
    //users for modifications
    final long retryBackoffMs = SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MS;
    final long retryBackoffMaxMs = SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
    final Integer loginConnectTimeoutMs = null;
    final Integer loginReadTimeoutMs = null;

    // Get client ssl configs if configured
    final JaasOptionsUtils jaasOptionsUtils = new JaasOptionsUtils(
        KsqlClientConfig.getClientSslConfig(configs));

    SSLSocketFactory socketFactory = null;
    final URL tokenEndpointUrl = configUtils.validateUrl(
        KsqlClientConfig.BEARER_AUTH_TOKEN_ENDPOINT_URL);

    if (jaasOptionsUtils.shouldCreateSSLSocketFactory(tokenEndpointUrl)) {
      socketFactory = jaasOptionsUtils.createSSLSocketFactory();
    }

    return new HttpAccessTokenRetriever(clientId, clientSecret, scope, socketFactory,
        tokenEndpointUrl.toString(), retryBackoffMs, retryBackoffMaxMs,
        loginConnectTimeoutMs, loginReadTimeoutMs, false);
  }

  private AccessTokenValidator getAccessTokenValidator(final Map<String, ?> configs) {
    final String scopeClaimName = KsqlClientConfig.getBearerAuthScopeClaimName(configs);
    final String subClaimName = KsqlClientConfig.getBearerAuthSubClaimName(configs);
    return new LoginAccessTokenValidator(scopeClaimName, subClaimName);
  }

  private OAuthTokenCache getOAuthTokenCache(final Map<String, ?> configs) {
    final short cacheExpiryBufferSeconds = KsqlClientConfig
        .getBearerAuthCacheExpiryBufferSeconds(configs);
    return new OAuthTokenCache(cacheExpiryBufferSeconds);
  }
}
