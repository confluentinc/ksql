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

import io.confluent.ksql.security.KsqlClientConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class ClientSecretIdpConfig implements IdpConfig {
  private String idpTokenEndpointUrl = "";
  private String idpClientId = "";
  private String idpClientSecret = "";
  private String idpScope = "";
  private String idpScopeClaimName = "scope";
  private String idpSubClaimName = "sub";
  private Short idpCacheExpiryBufferSeconds = 300;

  @Override
  public Map<String, Object> getIdpConfigs() {
    Map<String, Object> mp = new HashMap<>();
    mp.put(KsqlClientConfig.BEARER_AUTH_TOKEN_ENDPOINT_URL, idpTokenEndpointUrl);
    mp.put(KsqlClientConfig.BEARER_AUTH_CLIENT_ID, idpClientId);
    mp.put(KsqlClientConfig.BEARER_AUTH_CLIENT_SECRET, idpClientSecret);
    mp.put(KsqlClientConfig.BEARER_AUTH_SCOPE, idpScope);
    mp.put(KsqlClientConfig.BEARER_AUTH_SCOPE_CLAIM_NAME, idpScopeClaimName);
    mp.put(KsqlClientConfig.BEARER_AUTH_SUB_CLAIM_NAME, idpSubClaimName);

    return mp;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.idpTokenEndpointUrl = (String) configs.get(KsqlClientConfig.BEARER_AUTH_TOKEN_ENDPOINT_URL);
    this.idpClientId = (String) configs.get(KsqlClientConfig.BEARER_AUTH_CLIENT_ID);
    this.idpClientSecret = (String) configs.get(KsqlClientConfig.BEARER_AUTH_CLIENT_SECRET);
    this.idpScope = (String) configs.get(KsqlClientConfig.BEARER_AUTH_SCOPE);
    this.idpScopeClaimName = (String) configs.get(KsqlClientConfig.BEARER_AUTH_SCOPE_CLAIM_NAME);
    this.idpSubClaimName = (String) configs.get(KsqlClientConfig.BEARER_AUTH_SUB_CLAIM_NAME);
  }

  // Static builder class
  public static class Builder {
    private final ClientSecretIdpConfig config = new ClientSecretIdpConfig();

    public Builder withTokenEndpointUrl(final String url) {
      config.idpTokenEndpointUrl = url;
      return this;
    }

    public Builder withClientId(final String clientId) {
      config.idpClientId = clientId;
      return this;
    }

    public Builder withClientSecret(final String clientSecret) {
      config.idpClientSecret = clientSecret;
      return this;
    }

    public Builder withScope(final String scope) {
      config.idpScope = scope;
      return this;
    }

    public Builder withScopeClaimName(final String scopeClaimName) {
      config.idpScopeClaimName = scopeClaimName;
      return this;
    }

    public Builder withSubClaimName(final String subClaimName) {
      config.idpSubClaimName = subClaimName;
      return this;
    }

    public Builder withCacheExpiryBufferSeconds(
        final Short cacheExpiryBufferSeconds
    ) {
      config.idpCacheExpiryBufferSeconds = cacheExpiryBufferSeconds;
      return this;
    }

    public IdpConfig build() {
      return config;
    }
  }

  public String getIdpTokenEndpointUrl() {
    return idpTokenEndpointUrl;
  }

  public String getIdpClientId() {
    return idpClientId;
  }

  public String getIdpClientSecret() {
    return idpClientSecret;
  }

  public String getIdpScope() {
    return idpScope;
  }

  public String getIdpScopeClaimName() {
    return idpScopeClaimName;
  }

  public String getIdpSubClaimName() {
    return idpSubClaimName;
  }

  public Short getIdpCacheExpiryBufferSeconds() {
    return idpCacheExpiryBufferSeconds;
  }

  public IdpConfig copy() {
    return new Builder()
        .withTokenEndpointUrl(idpTokenEndpointUrl)
        .withClientId(idpClientId)
        .withClientSecret(idpClientSecret)
        .withScope(idpScope)
        .withScopeClaimName(idpScopeClaimName)
        .withSubClaimName(idpSubClaimName)
        .withCacheExpiryBufferSeconds(idpCacheExpiryBufferSeconds)
        .build();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ClientSecretIdpConfig)) {
      return false;
    }
    final IdpConfig ClientSecretIdpConfig = (ClientSecretIdpConfig) o;
    return Objects.equals(idpTokenEndpointUrl, ClientSecretIdpConfig.idpTokenEndpointUrl)
        && Objects.equals(idpClientId, ClientSecretIdpConfig.idpClientId)
        && Objects.equals(idpClientSecret, ClientSecretIdpConfig.idpClientSecret)
        && Objects.equals(idpScope, ClientSecretIdpConfig.idpScope)
        && Objects.equals(idpScopeClaimName, ClientSecretIdpConfig.idpScopeClaimName)
        && Objects.equals(idpSubClaimName, ClientSecretIdpConfig.idpSubClaimName)
        && Objects.equals(idpCacheExpiryBufferSeconds, ClientSecretIdpConfig.idpCacheExpiryBufferSeconds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(idpTokenEndpointUrl, idpClientId,
        idpClientSecret, idpScope, idpScopeClaimName,
        idpSubClaimName, idpCacheExpiryBufferSeconds);
  }

  @Override
  public String toString() {
    return "IdpConfig{"
        + "idpTokenEndpointUrl='" + idpTokenEndpointUrl + '\''
        + ", idpClientId='" + idpClientId + '\''
        + ", idpClientSecret='" + idpClientSecret + '\''
        + ", idpScope='" + idpScope + '\''
        + ", idpScopeClaimName='" + idpScopeClaimName + '\''
        + ", idpSubClaimName='" + idpSubClaimName + '\''
        + ", idpCacheExpiryBufferSeconds=" + idpCacheExpiryBufferSeconds
        + '}';
  }
}
