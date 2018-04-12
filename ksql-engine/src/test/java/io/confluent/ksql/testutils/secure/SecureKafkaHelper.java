/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.testutils.secure;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.PlainLoginModule;

import java.util.HashMap;
import java.util.Map;

public final class SecureKafkaHelper {

  private static final String PLAIN_SASL_MECHANISM = "PLAIN";

  private SecureKafkaHelper() {
  }

  public static Map<String, Object> getSecureCredentialsConfig(final Credentials credentials) {
    final Map<String, Object> props = new HashMap<>();
    addSecureCredentialsToConfig(props, credentials);
    return props;
  }

  public static void addSecureCredentialsToConfig(final Map<String, Object> props,
                                                  final Credentials credentials) {
    addSecureCredentialsToConfig(props);
    props.put(SaslConfigs.SASL_JAAS_CONFIG, buildJaasConfig(credentials));
  }

  public static void addSecureCredentialsToConfig(final Map<String, Object> props) {
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name);
    props.put(SaslConfigs.SASL_MECHANISM, PLAIN_SASL_MECHANISM);
  }

  public static String buildJaasConfig(final Credentials credentials) {
    return PlainLoginModule.class.getName()
           + " required username=\"" + credentials.username
           + "\" password=\"" + credentials.password
           + "\";";
  }
}
