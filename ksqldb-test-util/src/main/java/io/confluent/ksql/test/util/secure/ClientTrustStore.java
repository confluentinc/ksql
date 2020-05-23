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

  private static final String TRUSTSTORE_PASSWORD = "password";

  // Trust store containing two certs, for internal.example.com and external.example.com
  private static final String BASE64_ENCODED_STORE_MULTIPLE =
      "/u3+7QAAAAIAAAACAAAAAgAUaW50ZXJuYWwuZXhhbXBsZS5jb20AAAFyPog9+gAFWC41MDkAAAOtMIID"
          + "qTCCApGgAwIBAgIEIhgtjDANBgkqhkiG9w0BAQsFADCBhDELMAkGA1UEBhMCVVMxCzAJBgNVBAgTAkNB"
          + "MQswCQYDVQQHEwJTRjEdMBsGA1UEChMUaW50ZXJuYWwuZXhhbXBsZS5jb20xHTAbBgNVBAsTFGludGVy"
          + "bmFsLmV4YW1wbGUuY29tMR0wGwYDVQQDExRpbnRlcm5hbC5leGFtcGxlLmNvbTAeFw0yMDA1MjIyMjMz"
          + "MzRaFw0zMDA1MjAyMjMzMzRaMIGEMQswCQYDVQQGEwJVUzELMAkGA1UECBMCQ0ExCzAJBgNVBAcTAlNG"
          + "MR0wGwYDVQQKExRpbnRlcm5hbC5leGFtcGxlLmNvbTEdMBsGA1UECxMUaW50ZXJuYWwuZXhhbXBsZS5j"
          + "b20xHTAbBgNVBAMTFGludGVybmFsLmV4YW1wbGUuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB"
          + "CgKCAQEAh4yPHPaoQ5QeKXWlvY2rdzunYDd8SpyDYoicZPzYrczbYRCF3O0wqdr1C7AtfegDGiFLZb14"
          + "AuMmpcLVN/RGBV4B6X9KsatAzftYPXm4OaY13k9vv4p3iikD/5gnmuCZMoEDVNkjSvD0RlpJazp6d77t"
          + "8rwZV0rsyB7AOxErt+tkh2NYE4lZuLDE62GEPkf0vve0RcUBZsxaLjDEloo738rS9BpIFVK305wPOcpF"
          + "iy59d3Cp06rzPdbScvug+2QXm/8iEMj1FUNb/Y38/WUV8tvSbYZAYNSrgfmMR6LIn27/IxwdY0K/uDXa"
          + "ainNapO0SrCPGqmeKRHPkt74I4188wIDAQABoyEwHzAdBgNVHQ4EFgQUKh0QIg9TeTPtBvFcVl9kQxFV"
          + "+fIwDQYJKoZIhvcNAQELBQADggEBAIRIy/v/rmL4G56JHlsruxObiWATYGbLed+BsDtLLe40UVOvSdV/"
          + "0b1kq1zX+vDFE/lBtwk6c0EzEjaUOIpzASVnLJ73KQZ0t87HFN1JtZBvorwUOplY7uvWSEDYJx1zIJDy"
          + "2o+QsTv6NeCMdVO2i/4bRNJ2CuL2yVIOLscf+wpqsXcrfndj6Pkgceb/4lOWwGWD6CsOtvF54TgX8o0K"
          + "jCjUtPH2nr5SUHshfTvCn3PqPBmWvCwLK2XkAWHtNaqbUoAXaFaB8CfiH2K8cm++H+5ZdkXbrW6zNUui"
          + "WnTxHiGW37LA6pmTMZcQpVhuZ+q+tGeFhUEvHilvCnZnIMZtlCEAAAACABRleHRlcm5hbC5leGFtcGxl"
          + "LmNvbQAAAXI+ipKfAAVYLjUwOQAAA60wggOpMIICkaADAgECAgQTURK6MA0GCSqGSIb3DQEBCwUAMIGE"
          + "MQswCQYDVQQGEwJVUzELMAkGA1UECBMCQ0ExCzAJBgNVBAcTAlNGMR0wGwYDVQQKExRleHRlcm5hbC5l"
          + "eGFtcGxlLmNvbTEdMBsGA1UECxMUZXh0ZXJuYWwuZXhhbXBsZS5jb20xHTAbBgNVBAMTFGV4dGVybmFs"
          + "LmV4YW1wbGUuY29tMB4XDTIwMDUyMjIyMzc1NVoXDTMwMDUyMDIyMzc1NVowgYQxCzAJBgNVBAYTAlVT"
          + "MQswCQYDVQQIEwJDQTELMAkGA1UEBxMCU0YxHTAbBgNVBAoTFGV4dGVybmFsLmV4YW1wbGUuY29tMR0w"
          + "GwYDVQQLExRleHRlcm5hbC5leGFtcGxlLmNvbTEdMBsGA1UEAxMUZXh0ZXJuYWwuZXhhbXBsZS5jb20w"
          + "ggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCMIe+F4+EjIj38VbH5gWtaGd7XQmpnqVQbslcR"
          + "jyuQw2aiHw4xAnpA5+MOCLw0XJAnib9Ze2WtRalHr7erxWkeZhG4yETimbtl7vtmiMIlJMDdxiS1w4MB"
          + "LKcOhQFaHJgNmxHUq4T+W3sVJ3zfO7QJiPLngmuNwcOIjpObpfJ7G03iTUrETAtMbejgIWFj5Rsruvi+"
          + "REgAZbrr6ittf/s4590HZm3MuQ0WRJGb3y/Ns0mH6ISnxNAbz240R4xT07cRHvHKjU+xClE2VkDYtLRR"
          + "XqDOEe4anF1scOqSFwwr8eECBWSxwb2rdlf1hsB1d3Ch77RLVU0IoWMaX12+TPV7AgMBAAGjITAfMB0G"
          + "A1UdDgQWBBSrQDZ9N7v3dQB8CNa1mcj4RfkqrTANBgkqhkiG9w0BAQsFAAOCAQEAK/Q7+jQRFF6UNkRw"
          + "lVtXuDag0GJQb3RdPG0mNmOiQYcTELmwrd/+mfF96kSDLM0BBNcvBKIaKdpfXKdulpbw9u1x2dp1CH1q"
          + "LfJWPduPmfXZpGDwAnR35rcbr9Ca0cEF7uHH2lo3VUC9QFm5MT3/TqwkIZ1IJYOjyePPMM7iqh1cPb4C"
          + "kEe3zdSite5WGSL5kHSFnqDUeXDfGly5mpYPtpAOrn3ctL7TkYc73YQ/7B6eoU2JcQOs0yRiTMYnduel"
          + "flMQHzl1mmOjyJAfuBuRbVDP+UDzqx4/DOpzPRDTdwQCpMCdgQA3COdjWcDCkzWw19u2qDUFx5vTOTd8"
          + "EOnaLibQnyXD8bH+lz0gr3kCqit8U4F/";

  private static final String TRUSTSTORE_PASSWORD_MULTIPLE = "password";

  private static final AtomicReference<Path> trustStorePath = new AtomicReference<>();
  private static final AtomicReference<Path> trustStorePathMultiple = new AtomicReference<>();

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
   * @return props consumers and producers will need to connect to a secure Kafka cluster over SSL.
   */
  public static Map<String, String> trustStoreMultipleCertsProps() {
    return ImmutableMap.of(
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStorePathMultiple(),
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TRUSTSTORE_PASSWORD_MULTIPLE,
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

      return KeyStoreUtil.createTemporaryStore("client-trust-store", BASE64_ENCODED_STORE);
    });

    return path.toAbsolutePath().toString();
  }

  /**
   * @return the path to the temporary trust store.
   */
  public static String trustStorePathMultiple() {
    final Path path = trustStorePathMultiple.updateAndGet(existing -> {
      if (existing != null) {
        return existing;
      }

      return KeyStoreUtil.createTemporaryStore("client-trust-store-multiple",
          BASE64_ENCODED_STORE_MULTIPLE);
    });

    return path.toAbsolutePath().toString();
  }
}
