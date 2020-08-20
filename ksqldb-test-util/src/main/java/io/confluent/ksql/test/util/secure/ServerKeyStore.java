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
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper for creating a client key store to enable SSL in tests.
 */
public final class ServerKeyStore {

  private static final Logger log = LoggerFactory.getLogger(ServerKeyStore.class);

  private static final String BASE64_ENCODED_STORE =
      "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"
      + "XA==";

  private static final String EXPIRED_BASE64_ENCODED_STORE =
      "/u3+7QAAAAIAAAABAAAAAQADYmFkAAABcmJlWWEAAAUBMIIE/TAOBgorBgEEASoCEQEBBQAEggTp5B9lmw0sNM0Db50V"
      + "zcdYaSEfkbDWyaTp4syeFm9srNGcYmO1LWCToCMXLcQXeyaMaSeOaUXXPv4BhpFUO6l7Za7pKYwKvZoV6lSsvMBttI"
      + "7hY8hCIqGvqbXn6a7gFyEYnZ8lKxhQr4AcOsy6c5tE3gqvVABlB98PTFNNlNR9wDHfeyXDNf5gHLC2hHNZz+jaCu5w"
      + "8xEmQ7EWTMsW0Li/adUYkx/zicQKwU2bGGKuYuahDZ5OZE4Fa6OE4cOk2jCOeQSQn4S0g1ZtwCHq6XjAYUHG1NlFaq"
      + "5WJM7qGiLO+05qIoyL8ju7F7v+GVgerwRKyinmEOPmUZxKewG2DCbX+m/Hly+vjPpNjlwa1o93UJAyW4QZ85/wJU8u"
      + "vLJXWqSVlJb+97XgJTKQ9nzXvGEOqCXBbAv7l3IqFahgCC/3dikTqkWLZH9V8rypdtOf9/W1X8KAQBvTP5NSjW/kaz"
      + "q/e7ZmiP0+NpS2sjzGdPyGqO6DzATzJ+Jg8a7jyJg496bV0QbGq6XHaX4+XiPtkaB8zP+jeWFw/NEFnIpfROIiY4/t"
      + "u+fbH/VVcb4rhVchdM57pTZLnQHPN5tvxlDmOoXSzUojGAZnQRg97ldFmUk5QlXB2hJhQ33Wg+Py81iFKqeJEevgGU"
      + "DCVZ8/xm52mRID0K1zXsKYW52P8Kf+C6MCBd9ccx/oW9ljQRPThrkw0NpSCGdzfL8pyXe4n3suWmysIMdi4kixQbcb"
      + "JKC59T+S5HtCiVoq24iZlI8vJS1slYm7LefQsqOIqg8Ib9VSi6bgWZEpng6yJYt0X33BUpNp6GAf+mntDLD2GEjz3B"
      + "6poZHmuqgEL9ysoBy1D/MmkquVVyQQDBDCojp/NtDikSxLJMBOUI456sOGnikFYcuyWRSsf1HE7VI5Nd3+lrVANpB4"
      + "dx6v8y9gduNeiL9k8pfXkWu83Hz4jS9SQZRavTD0Kmrg05ZmNbTu7kNbI0H0xLPcRK1NjeTtXYI4xCTLVy52aEosTp"
      + "QGbPU372d0vkav1zR47LA+kRuwreBp4jmMGq9QsiMDeqCz+wwIWP4jgLhucuNy5H9MLmSQLRD4i616sNXXu+/qPb9s"
      + "HL7LatpvoS5siEOZ1dGWdf8CfkPJtnVu1/SNsDPfz0agm96yG7v5d/BWModKaz1W+6AZGFHI4HPcFzGy5JTYe6qSlN"
      + "KNfuYG25Npvn8GdwEUsEqhApIlmoM5B+jJumjYT24flD1UZ8nMwxAOg7Rlgi+UdSjEAgesKrxjByjuLzqEB93BrMuY"
      + "ZNaT3I/fDC1pary8CyJTZfU5u37oErlgJJ2mB7GcuDzBoLn92mTHlRIXB5EjSjN69+ZUDS+0PQIhAWsUrp3dsbalLr"
      + "ijZtQJpO2xifp6/QjmKRwEfxZ4tLXst5YEEWUnPCazjrIvEbgZKikbN8skOuuVXNiTvNruAWbfk6t6PViuWfjnJyNv"
      + "DcXXF/UaH0EPW+FKTeleDGaE85mSRKvOqFp2fkMQZnCfZcEGy3BIutoRWJwDsqfy1FvxsZ+e2WZam6fVMH4D9NoXLk"
      + "Dm6vEGkpnAzwm9qszRiAGubiuTWWBfK+17T8axX5/lFEg7huBQAA/uL+GP86dkSY5icPo7IPkbC+1ozUespMSDEjQd"
      + "lp9Gge6nEoP6YlmnqZ5EnhdVNQfIBtiYZFYNAvnaAAAAAQAFWC41MDkAAANLMIIDRzCCAi+gAwIBAgIEJ95VKzANBg"
      + "kqhkiG9w0BAQsFADBUMQwwCgYDVQQGEwNiYWQxDDAKBgNVBAgTA2JhZDEMMAoGA1UEBxMDYmFkMQwwCgYDVQQKEwNi"
      + "YWQxDDAKBgNVBAsTA2JhZDEMMAoGA1UEAxMDYmFkMB4XDTIwMDUyOTIxNDUwOFoXDTIwMDUzMDIxNDUwOFowVDEMMA"
      + "oGA1UEBhMDYmFkMQwwCgYDVQQIEwNiYWQxDDAKBgNVBAcTA2JhZDEMMAoGA1UEChMDYmFkMQwwCgYDVQQLEwNiYWQx"
      + "DDAKBgNVBAMTA2JhZDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAIr/LFqkCmt/HKa5EmJVNrVv19hi7o"
      + "KPJvxGwj8qFTl5eXu1K+msOsz8CKTdYZsJnjScoIXvHJaI8sV1FkxgZJ5iGQWtZ2JoqzvFwRrtWjQtvHf0HOqayaE8"
      + "d2R31b3gACUFMw3MBOk5oZrumGT823NW8aEYuwZctcvNmJR2CjUvu4B8r8js2Yl/1ym8JMuXo4BzpFc6dpokMWBFSb"
      + "k7ccgnNQ6aiAmiheOUSs8vpRc6NorANaiGGHIH0vOCzSCwYyfnz6keapB6oBCriAdB8EoEw1mafQ4QgbeoAplXJ139"
      + "dY7t011z34wHumtfbCH0s8qzh9qUVJgUbA97nMQKJMsCAwEAAaMhMB8wHQYDVR0OBBYEFP+CdVtbliAqK/z/txCF/+"
      + "qg9hOEMA0GCSqGSIb3DQEBCwUAA4IBAQAMy/hbT9rvgu12TraF8JmrzdG+6NDlHAsFi1kjcYKAArF5BgoVunpVVhnc"
      + "YDFrQfKgEMf2v3+L3GNRsC7w1sWZmW3nlJHUCWDUV7MyWD1iDhEKwXVmRNNPXBhyqTbCmhifyQWtcf6i5WePv/8sVD"
      + "tJHjCGrFSEI4KEh44uWNcr0AN61ybTfIiyqj13QYbovUx37tE2dYy5bvZxcgbSYw01L1NRNecnUv6GsiZe6MFxANLt"
      + "Ie3ZnMjshmx1VRF1EAb9vAW5Y2E/hnXwnC4q0dpJPWotc6BZfU3aG1dtrQi0LvbtYJd2ivBCrBNyeUC7lWeCFuAXhR"
      + "pzVZmjiWWD9P/+PQmhOrEAKd+Yo64eW62WWV8htPg=";

  private static final String KEY_PASSWORD = "password";
  private static final String KEYSTORE_PASSWORD = "password";
  private static final String TRUSTSTORE_PASSWORD = "password";

  private final AtomicReference<Path> keyStorePath = new AtomicReference<>();
  private final AtomicReference<Path> clientKeyStorePath = new AtomicReference<>();

  public ServerKeyStore() {
  }

  /**
   * @return props brokers will need to connect to support SSL connections.
   *         The store at this path may be replaced with an expired store via the method
   *         {@link #loadExpiredServerKeyStore}.
   */
  public Map<String, String> keyStoreProps() {
    return ImmutableMap.of(
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStorePath(),
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, KEYSTORE_PASSWORD,
        SslConfigs.SSL_KEY_PASSWORD_CONFIG, KEY_PASSWORD,
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, keyStorePath(),
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TRUSTSTORE_PASSWORD
    );
  }

  /**
   * @return props clients may use to connect to support SSL connections.
   *         In contrast to {@link #keyStoreProps}, the store at this path will not replaced
   *         with an expired store when the method {@link #loadExpiredServerKeyStore} is called.
   */
  public Map<String, String> clientKeyStoreProps() {
    return ImmutableMap.of(
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, clientKeyStorePath(),
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, KEYSTORE_PASSWORD,
        SslConfigs.SSL_KEY_PASSWORD_CONFIG, KEY_PASSWORD,
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientKeyStorePath(),
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TRUSTSTORE_PASSWORD
    );
  }

  public void loadExpiredServerKeyStore() {
    KeyStoreUtil.putStore(Paths.get(keyStorePath()), EXPIRED_BASE64_ENCODED_STORE);
    log.info("Loaded expired store");
  }

  public void loadValidServerKeyStore() {
    KeyStoreUtil.putStore(Paths.get(keyStorePath()), BASE64_ENCODED_STORE);
    log.info("Loaded valid store");
  }

  private String keyStorePath() {
    final Path path = keyStorePath.updateAndGet(existing -> {
      if (existing != null) {
        return existing;
      }

      return KeyStoreUtil.createTemporaryStore("server-key-store", BASE64_ENCODED_STORE);
    });

    return path.toAbsolutePath().toString();
  }

  private String clientKeyStorePath() {
    final Path path = clientKeyStorePath.updateAndGet(existing -> {
      if (existing != null) {
        return existing;
      }

      return KeyStoreUtil.createTemporaryStore("client-key-store", BASE64_ENCODED_STORE);
    });

    return path.toAbsolutePath().toString();
  }
}
