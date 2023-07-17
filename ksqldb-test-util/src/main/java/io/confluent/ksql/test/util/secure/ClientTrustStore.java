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

  private static final String TRUSTSTORE_PASSWORD = "truststore-password";
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
