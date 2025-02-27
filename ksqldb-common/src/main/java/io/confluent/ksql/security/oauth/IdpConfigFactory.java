/*
 * Copyright 2025 Confluent Inc.
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
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.kafka.common.config.ConfigException;

@SuppressWarnings({"checkstyle:CyclomaticComplexity", "BooleanExpressionComplexity"})
public final class IdpConfigFactory {

  private IdpConfigFactory() {}

  public static IdpConfig getIdpConfig(
          final Map<String, ?> configs) {


    final String bearerAuthMethod =
            (String) configs.get(KsqlClientConfig.BEARER_AUTHENTICATION_METHOD);

    if (bearerAuthMethod == null || bearerAuthMethod.isEmpty()) {
      final ClientSecretIdpConfig clientSecretIdpConfig = new ClientSecretIdpConfig();
      try {
        clientSecretIdpConfig.configure(configs);
        return clientSecretIdpConfig;
      } catch (ConfigException e) {
        return null;
      }
    }

    final ServiceLoader<IdpConfig> serviceLoader = ServiceLoader.load(
            IdpConfig.class,
            IdpConfigFactory.class.getClassLoader()
    );

    for (IdpConfig idpConfig : serviceLoader) {
      if (idpConfig.getAuthenticationMethod().equals(bearerAuthMethod)) {
        idpConfig.configure(configs);
        return idpConfig;
      }
    }

    return null;
  }
}
