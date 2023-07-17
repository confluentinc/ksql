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

/**
 * Helper for creating a client key store to enable SSL in tests.
 */
public final class ServerKeyStore {
  /*
   Steps to re-create the truststore and keystore:

   Note: The first and last name asked during the keystore generation should be 'localhost', which
         will be used as the key alias.

   Keystore:
   - Generate the keystore with a keystore & key passwords (99 days validity).
     $ keytool -keystore keystore.jks -alias localhost -keyalg RSA -storetype JKS \
               -keypass key-password -storepass keystore-password -validity 36135 -genkey
   - Convert to base64
     $ openssl base64 -A -in keystore.jks

   Truststore:
   - Export public key/certificate from keystore
     $ keytool -export -alias localhost -keystore keystore.jks -rfc -file public.cert
   - Create truststore from public key/certificate
     $ keytool -import -alias localhost -file public.cert -storetype JKS \
               -storepass truststore-password -keystore truststore.jks
   - Convert to base64
     $ openssl base64 -A -in truststore.jks
  */


  private static final String BASE64_ENCODED_TRUSTSTORE =
      "/u3+7QAAAAIAAAABAAAAAgAJbG9jYWxob3N0AAABdmd5tvQABVguNTA5AAADfTCCA3kwggJhoAMCAQICBA4/S/IwDQY"
      + "JKoZIhvcNAQELBQAwbDELMAkGA1UEBhMCVVMxDjAMBgNVBAgTBVRleGFzMQ8wDQYDVQQHEwZBdXN0aW4xEjAQBg"
      + "NVBAoTCUNvbmZsdWVudDEUMBIGA1UECxMLRW5naW5lZXJpbmcxEjAQBgNVBAMTCWxvY2FsaG9zdDAgFw0yMDEyM"
      + "TUxNjAxNTBaGA8yMTE5MTEyMjE2MDE1MFowbDELMAkGA1UEBhMCVVMxDjAMBgNVBAgTBVRleGFzMQ8wDQYDVQQH"
      + "EwZBdXN0aW4xEjAQBgNVBAoTCUNvbmZsdWVudDEUMBIGA1UECxMLRW5naW5lZXJpbmcxEjAQBgNVBAMTCWxvY2F"
      + "saG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALgeKtCVSFtFY+ZnzY2CnzM44A2OsLpsIxAlPv"
      + "kLJ+hW1ld9B89Vayu1a5/lWTee0Fc2OZR6MC5zhkZmRaGxBB2Dl4XXFMUKdngJdkVh7+AOQqSNy89LdirnvEMgb"
      + "n9tk3zNQQR/K81uQt9sMBPjd7zRIj80PGu8pvBGwTfVvVI/GU9z0gGi3B8z8GwFvY3Zzy9KYeUUWk30f9WXx6jU"
      + "tjtck0g35Bm8CHCGAXgY05vkoGwDVrPgw9CDILlvRIF4PqyDOWzP8t7Sai4caLoU1Aa8rodHZJi5BYnFfFCVh6B"
      + "5kBstDkBuizgQ8RVaCjt+P3+AvnsatBUKiIlYC8rTFP8CAwEAAaMhMB8wHQYDVR0OBBYEFKUXrnfy9KAyf1ey9S"
      + "VGslYJoA/6MA0GCSqGSIb3DQEBCwUAA4IBAQAmo0bQpm3H9Yio7fvQUxrhrPd0jWjKG6ZUgqRNjQCknsDtj7RFc"
      + "JR7VhJHW+qSr3z0mS6QkGiOBnazKk3KEPrbEPJppKxmuBTFJMesC2fPBGCi5yq6iMymjlN+Uicb/gZvbtZ9bJ6C"
      + "Z+XUD8Xu9nu7z4DvADgHcLoduraFa6WptfTv6vwk+EKJoFTrWMZcUVupoQSh46hdQr2ePhTJtBpmtxhyOdROP/O"
      + "hYF5z/dTYHoYJdMExBIId8tyyMrnKp35mDsWF7k+vKciTCCwyf6E1zeTJr6tDJFYYsjPx0tB8i//rTyi0Mv2Mtv"
      + "8MvTNbzUpte61oAUJEXVDEH48OT2ejEgMJi8nTUi+2c+9A03FFiE21jlo=";

  private static final String BASE64_ENCODED_KEYSTORE =
      "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"
      + "BT+";

  private static final String EXPIRED_BASE64_ENCODED_KEYSTORE =
      "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"
       + "rUP7ew6gT/gSwmncHnSesD7L+IzM+2LVM=";

  private static final String KEY_PASSWORD = "key-password";
  private static final String KEYSTORE_PASSWORD = "keystore-password";
  private static final String KEYSTORE_ALIAS = "localhost";
  private static final String TRUSTSTORE_PASSWORD = "truststore-password";

  private final AtomicReference<Path> trustStorePath = new AtomicReference<>();
  private final AtomicReference<Path> clientTrustStorePath = new AtomicReference<>();
  private final AtomicReference<Path> keyStorePath = new AtomicReference<>();
  private final AtomicReference<Path> clientKeyStorePath = new AtomicReference<>();

  public ServerKeyStore() {
  }

  /**
   * @return props brokers will need to connect to support SSL connections.
   *         The store at this path may be replaced with an expired store via the method
   *         {@link #writeExpiredServerKeyStore}.
   */
  public Map<String, String> keyStoreProps() {
    return ImmutableMap.of(
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStorePath(),
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, KEYSTORE_PASSWORD,
        SslConfigs.SSL_KEY_PASSWORD_CONFIG, KEY_PASSWORD,
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStorePath(),
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TRUSTSTORE_PASSWORD
    );
  }

  /**
   * @return props clients may use to connect to support SSL connections.
   *         In contrast to {@link #keyStoreProps}, the store at this path will not replaced
   *         with an expired store when the method {@link #writeExpiredServerKeyStore} is called.
   */
  public Map<String, String> clientKeyStoreProps() {
    return ImmutableMap.of(
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, clientKeyStorePath(),
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, KEYSTORE_PASSWORD,
        SslConfigs.SSL_KEY_PASSWORD_CONFIG, KEY_PASSWORD,
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStorePath(),
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TRUSTSTORE_PASSWORD
    );
  }

  public String getKeyAlias() {
    return KEYSTORE_ALIAS;
  }

  public void writeExpiredServerKeyStore() {
    KeyStoreUtil.putStore(
        "expired-server-key-store",
        Paths.get(keyStorePath()),
        EXPIRED_BASE64_ENCODED_KEYSTORE
    );
  }

  public void writeValidServerKeyStore() {
    KeyStoreUtil.putStore(
        "valid-server-key-store",
        Paths.get(keyStorePath()),
        BASE64_ENCODED_KEYSTORE
    );
  }

  private String keyStorePath() {
    final Path path = keyStorePath.updateAndGet(existing -> {
      if (existing != null) {
        return existing;
      }

      return KeyStoreUtil.createTemporaryStore("server-key-store", BASE64_ENCODED_KEYSTORE);
    });

    return path.toAbsolutePath().toString();
  }

  private String trustStorePath() {
    final Path path = trustStorePath.updateAndGet(existing -> {
      if (existing != null) {
        return existing;
      }

      return KeyStoreUtil.createTemporaryStore("server-trust-store", BASE64_ENCODED_TRUSTSTORE);
    });

    return path.toAbsolutePath().toString();
  }

  private String clientKeyStorePath() {
    final Path path = clientKeyStorePath.updateAndGet(existing -> {
      if (existing != null) {
        return existing;
      }

      return KeyStoreUtil.createTemporaryStore("client-key-store", BASE64_ENCODED_KEYSTORE);
    });

    return path.toAbsolutePath().toString();
  }

  private String clientTrustStorePath() {
    final Path path = clientTrustStorePath.updateAndGet(existing -> {
      if (existing != null) {
        return existing;
      }

      return KeyStoreUtil.createTemporaryStore("client-trust-store", BASE64_ENCODED_TRUSTSTORE);
    });

    return path.toAbsolutePath().toString();
  }
}
