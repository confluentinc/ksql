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

  private static final String BASE64_ENCODED_STORE =
      "/u3+7QAAAAIAAAABAAAAAgAMY2xpZW50LWFsaWFzAAABcfZVSf8ABVguNTA5AAADaTCCA2UwggJNoAMC"
          + "AQICBAV2hUYwDQYJKoZIhvcNAQELBQAwYzELMAkGA1UEBhMCVVMxCzAJBgNVBAgTAkNBMQswCQYDVQQH"
          + "EwJTRjESMBAGA1UEChMJbG9jYWxob3N0MRIwEAYDVQQLEwlsb2NhbGhvc3QxEjAQBgNVBAMTCWxvY2Fs"
          + "aG9zdDAeFw0yMDA1MDgyMjA3MTlaFw0yMDA4MDYyMjA3MTlaMGMxCzAJBgNVBAYTAlVTMQswCQYDVQQI"
          + "EwJDQTELMAkGA1UEBxMCU0YxEjAQBgNVBAoTCWxvY2FsaG9zdDESMBAGA1UECxMJbG9jYWxob3N0MRIw"
          + "EAYDVQQDEwlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCkDtseMXsxziNU"
          + "GbxPoHmXUA0hgRXbZNRRMCYAOUtnJdVuPfGy52k9W+LAbGzZdkBqfigWDGqsZaZurgZt9B02OG8skPaG"
          + "nOTrZjU8QU6axlT/sMu5zoAklWYKPqPhWlEnr95N2fqdV4NMJIqKC29l5hFnS2EFtQehmRnKwE8e1nAD"
          + "opbt1GCsNjLMaqwydgnuHKvtKrI4ETuEy6SrJFqDurGtPmckEln+84VfNARUFiYGzK/CZe5v8IJ3336R"
          + "fVj4uWMUpHWn/kW1NRckbXLu0nGoB69FbO5BLcSnh20tsHHxALMW2gLre/RH16B94ckFzpNmkzwN8INJ"
          + "LMLU2gTHAgMBAAGjITAfMB0GA1UdDgQWBBSVwdS4DtRDYdIvj9jBpvz1u1lfIjANBgkqhkiG9w0BAQsF"
          + "AAOCAQEAi7ZJ5l0empUuU37AEN+l4Fu6/pGfrIU29OIpJDWXbwRBMI9UIMlTQFg4IOy3IOXp1Myt96u2"
          + "ctSe+lo4mjGovS+2ezRXpsP7bSZucUmxi92e7+4TkpDHiyOXCvyaIdlmhSUE0pIh5JTPSyJnn3qUrwgt"
          + "7kej+uQ3Xm5HhqqSkrAOBcA+5UWS63Uc6ZUk2GVZwYSeTPRC1+2doBgVTZBnez3+bBieADvhoLO8Oa4C"
          + "dY9iktPE4URVGo60LS3b5uaAs0i2uW+J06FKP/xXG0fRS1p9NGQYMCHLaY+M9cnW8kWbZZqPpShaIjc/"
          + "uVuKg9CRKUiWh8wP7RxkLMgTxkvYypqYuEjivrbru8nTXDaXBQLQ/E02";

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

      return KeyStoreUtil.createTemporaryStore("client-trust-store", BASE64_ENCODED_STORE);
    });

    return path.toAbsolutePath().toString();
  }
}
