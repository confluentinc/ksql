/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.test.util.secure;

import com.google.common.collect.ImmutableMap;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.config.SslConfigs;

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

  private static final String
      BASE64_ENCODED_STORE2 = "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";

  private static final String TRUSTSTORE_PASSWORD = "changeit";
  private static final AtomicReference<Path> trustStorePath = new AtomicReference<>();

  private ClientTrustStore() {
  }

  /**
   * @return props consumers and producers will need to connect to a secure Kafka cluster over SSL.
   */
  public static Map<String, String> trustStoreProps() {
    return ImmutableMap.of(
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStorePath(),
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword(),
        SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""
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

      return KeyStoreUtil.createTemporaryStore("client-trust-store", BASE64_ENCODED_STORE2);
    });

    return path.toAbsolutePath().toString();
  }
}
