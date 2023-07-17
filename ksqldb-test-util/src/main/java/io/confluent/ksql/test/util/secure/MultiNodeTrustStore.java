/*
 * Copyright 2020 Confluent Inc.
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

public final class MultiNodeTrustStore {
  // Trust store containing certs for node-1.example.com and node-2.example.com
  private static final String BASE64_ENCODED_STORE_NODE1_NODE2 =
      "/u3+7QAAAAIAAAACAAAAAgASbm9kZS0yLmV4YW1wbGUuY29tAAABclh6tp0ABVguNTA5AAADnzCCA5sw"
          + "ggKDoAMCAQICBFc8YvkwDQYJKoZIhvcNAQELBQAwfjELMAkGA1UEBhMCVVMxCzAJBgNVBAgTAkNBMQsw"
          + "CQYDVQQHEwJTRjEbMBkGA1UEChMSbm9kZS0yLmV4YW1wbGUuY29tMRswGQYDVQQLExJub2RlLTIuZXhh"
          + "bXBsZS5jb20xGzAZBgNVBAMTEm5vZGUtMi5leGFtcGxlLmNvbTAeFw0yMDA1MjcyMzE1MzRaFw0zMDA1"
          + "MjUyMzE1MzRaMH4xCzAJBgNVBAYTAlVTMQswCQYDVQQIEwJDQTELMAkGA1UEBxMCU0YxGzAZBgNVBAoT"
          + "Em5vZGUtMi5leGFtcGxlLmNvbTEbMBkGA1UECxMSbm9kZS0yLmV4YW1wbGUuY29tMRswGQYDVQQDExJu"
          + "b2RlLTIuZXhhbXBsZS5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDC7ya6y8BKCMuh"
          + "BpnPsnIOcl+EebVH4z75IzzQojRYzAqIeIJr4osAWRLpCp9ZEgxPnv0iS1RQL1zNOuPDU4OScQlLGI+b"
          + "3eeg2lBfu+s1ww42J7oOINpWEpg3ue3QDeTDrnzN/wgPHgTMlg96QHVFA5ZR/d1HY0Y05jf54KKwT0g7"
          + "sZ92zAcWToRlc8/uo166Q7czMPiG1JspOXgLU8IYZhoPceiBtUmAwbVf4sdnAoFknPQPzsZZIRwSuCBF"
          + "Hk6CLrJG6J84aSOh8U0qtuRUFnr9XzgArmKaVviGjQcuni9Boc0TngE63MYbTEgxzESpOrvSBXoETdDe"
          + "3rDDXkiVAgMBAAGjITAfMB0GA1UdDgQWBBSMV1OY7UpwYb0iMEpeiBYE9ltAyDANBgkqhkiG9w0BAQsF"
          + "AAOCAQEAGox+Wge6B+D46HVxhfqQDSV4PutqwU9W1J1nHGB32Tv/4svYLkjDQb6OgbEQNSYLFvLHGjFC"
          + "ARn4WviwgmTDdkOSmpfWf1xwy9/kgPWa77jdrLQpaitWPUSVYdWPezjV2CljD9qnVJKaw1wcAhhgoqbO"
          + "xvBpXF4KeUMiCS4tQqvKfOS3B4unc4Dy/wUqDzhc2A+yO3+C0r6ylCfoahB4wg1Bo+TEgOgQKzmJJ64I"
          + "4n1Sdj7jTZPPRt1sGhck5kYeSZXvAvpNi1goBY3+k6J+DvM3ORnBQLbL8RDy2ZPN7kuIZfzPe8jeP6Re"
          + "3ex7zJjuoBIngeLJVG2o/2rg4GuPbQAAAAIAEm5vZGUtMS5leGFtcGxlLmNvbQAAAXJYencLAAVYLjUw"
          + "OQAAA58wggObMIICg6ADAgECAgRypUGoMA0GCSqGSIb3DQEBCwUAMH4xCzAJBgNVBAYTAlVTMQswCQYD"
          + "VQQIEwJDQTELMAkGA1UEBxMCU0YxGzAZBgNVBAoTEm5vZGUtMS5leGFtcGxlLmNvbTEbMBkGA1UECxMS"
          + "bm9kZS0xLmV4YW1wbGUuY29tMRswGQYDVQQDExJub2RlLTEuZXhhbXBsZS5jb20wHhcNMjAwNTI3MjMx"
          + "MTE0WhcNMzAwNTI1MjMxMTE0WjB+MQswCQYDVQQGEwJVUzELMAkGA1UECBMCQ0ExCzAJBgNVBAcTAlNG"
          + "MRswGQYDVQQKExJub2RlLTEuZXhhbXBsZS5jb20xGzAZBgNVBAsTEm5vZGUtMS5leGFtcGxlLmNvbTEb"
          + "MBkGA1UEAxMSbm9kZS0xLmV4YW1wbGUuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA"
          + "i8OEwJ4SMrFUhmJM7DyexGLO7yjAFDGPmWFjfNZgFANp619TS2S7hjcapAFSXSK9VZK6RjxzI9ALiUW1"
          + "BAa5oBAMmtTNyQyLu08AydqgnGgIefEIdHLVMj+DXvNT22h+byJQmiEQhXFk7Vj+Z7afnPxuiSEQ//4v"
          + "djRQFbFbIW4+/2lspbmJJFsEsg2xaLTnysMoKpAc8WCVf2Hqhi9WND5XO7Gxdusn3iJslOaGkSNrMRj6"
          + "6YrHDr4tKFL2EletGxMbT9wH3hlaj7E2Jd0KZlY+cAq3R9b2o3fFfJc0b1+Kn0UoqI7lFS8PCMAaG7C2"
          + "JC3EMHiWcsk0u2p5rmOffQIDAQABoyEwHzAdBgNVHQ4EFgQU9Hvd/Bgc4Qhv8w2Dzjq+2vHp8T8wDQYJ"
          + "KoZIhvcNAQELBQADggEBAAt6NbOF+ij60o0lzCrNabwi7Urp6H1aaVIDBy51a3Q6MjKG5CRaFMtecj9O"
          + "kYxmpeFKjFLodhxthu6chD73AErBIpHl44ZnJoOYjfvkLubJmbgzAhpwwJmxOwpNFSpA6j+ipVUwbYU+"
          + "3RUx6lxSUylmxpjSolqr90i8J2B5QDJ5NTqU4Ix2HNHJYh1wGmZdn2k40oS/x8zEWjwfAad02RebFplX"
          + "18cMT9AcaR7L0Ci96K9HKVE3c1OoW3d7G8CcYfS/oLiRrs9d7IkOCpdt+d/Z+Qz7sYcPiZsck+gLFTjp"
          + "EBAdMmZ4kEEJpQmEOI8AFgE+9di/QmO/9w7k8Dfl9eTQh257dB1GVg12kJ4Sn+kgwV/ZmA==";

  private static final String TRUSTSTORE_PASSWORD = "password";

  private static final AtomicReference<Path> trustStorePath = new AtomicReference<>();

  private MultiNodeTrustStore() {}

  /**
   * @return props consumers and producers will need to connect to a secure Kafka cluster over SSL.
   */
  public static Map<String, String> trustStoreNode1Node2Props() {
    return ImmutableMap.of(
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStorePathNode1Node2(),
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TRUSTSTORE_PASSWORD,
        SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""
    );
  }

  /**
   * @return the path to the temporary trust store.
   */
  public static String trustStorePathNode1Node2() {
    final Path path = trustStorePath.updateAndGet(existing -> {
      if (existing != null) {
        return existing;
      }

      return KeyStoreUtil.createTemporaryStore("client-trust-store-node1",
          BASE64_ENCODED_STORE_NODE1_NODE2);
    });

    return path.toAbsolutePath().toString();
  }
}
