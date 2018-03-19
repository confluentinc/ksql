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

import com.google.common.collect.ImmutableMap;

import org.apache.kafka.common.config.SslConfigs;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Helper for creating a client trust store to enable SSL in tests.
 */
public final class ClientTrustStore {
  private static final String
      BASE64_ENCODED_STORE =
      "/u3+7QAAAAIAAAABAAAAAgAGY2Fyb290AAABYgp6gD0ABVguNTA5AAADLDCCAygwggIQAgkAvZW/3jNCgKgwDQYJKoZI"
      + "hvcNAQELBQAwVTELMAkGA1UEBhMCVUsxDzANBgNVBAgMBkxvbmRvbjEPMA0GA1UEBwwGTG9uZG9uMRUwEwYDVQQKDA"
      + "xjb25mbHVlbnQuaW8xDTALBgNVBAsMBEtTUUwwIBcNMTgwMzA5MTExNjM1WhgPMjExODAyMTMxMTE2MzVaMFUxCzAJ"
      + "BgNVBAYTAlVLMQ8wDQYDVQQIDAZMb25kb24xDzANBgNVBAcMBkxvbmRvbjEVMBMGA1UECgwMY29uZmx1ZW50LmlvMQ"
      + "0wCwYDVQQLDARLU1FMMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAr1aITumi00PI6Wne/IXuv98alM/Q"
      + "3pIwpWr+NZ/mbF3DbF9yDxKPuL2O6pjn1n7Hex1uX/xN8zqaC23bkUWKx0yFGaCk5MZaeNV3kHWanqgwN3O+foIhtQ"
      + "pN0/UJPDgqPJczm2AY7bqto3p6Kicdqle/waOwRsk69gF+azxx5lpZN+kSbOV5AEyprDd6xYQPz9U8hJMmwWFhNUTp"
      + "3IHNBpVlE/VLLLUJt/tMNTc67+iVcUG7j+ZrEq46MLcToOcoeFwHpCBTxkwk3Z3696lJLQh5wVKgEqxYCSYOf4thoN"
      + "gmKmSWH3zcH7IwD53g1g2aqPQQh0GfvNkjoaS6/zoZjQIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQBYrI+EdXTeL5mA"
      + "t1Xe0zpttZ+QelCrEsGiVDj7KynNbi1rcleNQ9uG0uAIFCp8qYh8QJVxVoiMMOtq9vBs9kfRPfxqb00T4scvi4W3cT"
      + "IWd1/m6PNkWM634rZpfyawiOaGQKMaPD+G75hHSZKqCPQmaEWo0rjUJSyGQ34ZNCmx/awMODQVNKRRKQvHIjc2HPh2"
      + "IbJLvr32wjiVsAwc1xRG9XMHTcmqIJkQzw9AqASNi7uYFd0gwBHEuA2NZ34zznI9hzjeL/b7l/9FCtVCtM64wZA+IF"
      + "7rKeqVDlpM5lUidfckmrF3TxmS6d1aN/1WSZtDZCnNK8+h6LrrbtSQMsS6tc1Cv5YjJ/7KB+rQmTmGJCdzI5E=";

  private static final String TRUSTSTORE_PASSWORD = "password";
  private static final AtomicReference<Path> trustStorePath = new AtomicReference<>();

  private ClientTrustStore() {
  }

  /**
   * @return props consumers and producers will need to connect to a secure Kafka cluster over SSL.
   */
  public static Map<String, ?> trustStoreProps() {
    return ImmutableMap.of(
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStorePath(),
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword()
    );
  }

  /**
   * @return the password used to secure the trust store.
   */
  public static String trustStorePassword() {
    return TRUSTSTORE_PASSWORD;
  }

  /**
   * @return the path to the temporary trust store.
   */
  public static String trustStorePath() {
    final Path path = trustStorePath.updateAndGet(existing -> {
      if (existing != null) {
        return existing;
      }

      return KeyStoreUtil.createTemporaryStore("client-trust-store", BASE64_ENCODED_STORE);
    });

    return path.toAbsolutePath().toString();
  }
}
