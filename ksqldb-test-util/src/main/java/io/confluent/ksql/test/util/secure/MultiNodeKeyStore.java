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

public final class MultiNodeKeyStore {
  // Keystore containing cert/key for node-1.example.com
  private static final String BASE64_ENCODED_STORE_NODE1 =
      "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";

  // Keystore containing cert/key for node-2.example.com
  private static final String BASE64_ENCODED_STORE_NODE2 =
      "MIIKawIBAzCCCiQGCSqGSIb3DQEHAaCCChUEggoRMIIKDTCCBYEGCSqGSIb3DQEHAaCCBXIEggVuMIIF"
          + "ajCCBWYGCyqGSIb3DQEMCgECoIIE+zCCBPcwKQYKKoZIhvcNAQwBAzAbBBQJTlOl6Udn4FPQjST6hV17"
          + "tAOcKQIDAMNQBIIEyLZ5KkIWUUG66k5W0afBcEls8l6yQw05efxIg5NP1LPDwCILgO5k2ApbXwmulzMu"
          + "a8briiAReO9nxgI1dmdZ7xevyXoSQRp9/F+YAmXNPG2i0a/nSkttvYF/+MBL6KZ1UhChdedvbwiqrbMv"
          + "U32TomCGcltYLyhMODyt7ZF+iTT8UT+Zc/rnMVRcuKfbp/GhVUJB1PooSZRvG9YSqYL7f7NH1Ka3sWg6"
          + "EtkMD+iyvZx7G8W+VYt0QJUg9TnfK2/+hTq7Wobh8g0Hyw3VaLpNZgkeyDjYfh7hR5Fclaa3pplX4JZC"
          + "w5GZkt2HIY1YQfPXGWq+tkpRBTR06ZUWAidxsiKaCI48BXYLgM9QOTCWcuZCVQWAthax3KKhTbR5d1qV"
          + "4ryXDZXZvTIv2OJPvpp6En+MRxbK+khXo1XHwv5tCPB57H0r9gsMSm6t/7y10PQtxocAo4XDf2xMgR2F"
          + "wJ675LpOzbFPOgD/SYLDdRgIAZhwewpWR2Kfm+6IYmsEQ/VhbkmDVFViuJlMS985SnpNn/2HuKPvYWKs"
          + "O2zqTC+VHkp/qA2r3s+tFLTcYHIJwTwrDv3cxsY/Fia5WAwpjW1ZoBMTP+p3fOB+DVrtBwfZKkLcZfOu"
          + "7RLnkowYZ9epssTxxeqaYR4HSPQd3WrdnB8ZD5Y/slmC5VmyvQGRY94Z5060Pf/SDOQo1Gaisqv9AuXG"
          + "FLRdSjYMN7o4rsHBaH0o/KPyH4dolmAAOotFqpihEb4qrELuPApnvUtk5axkkMEiAOXRd+SUEfPgwtx3"
          + "O9Cg3A61wKZ/HMZs+swSDWVxyVXC7kW0djOGi370Xgb2DF4biUfby5itKqvZLB92n4qZ6NB59ljnKDZo"
          + "OZRwSprO9hbRJnVf+fu4IGyb5RXaxDsvxTuNDd8Ahk9WjViOD1owW9UNkVbQZ6uTVZj+h7QZnQ22I+uz"
          + "pWBddnMEeFmJM2lmxAtW3uzq5SikfGByKXzPEdGVXlvuX8tHFkme8KDQV6w8IskoW2t14CpwIj+PCpA0"
          + "CV3wOPXH69WL76pdVXyejVVn3XR3rD8VSc7UCB6qKWucge7F/u/6VOAlw+y0+N+VEUP58o6kGeT6w8/b"
          + "g7kM3wx+1OQ2kDwyZBHNdcVc6Tgorod7Kasbb03cYEynZUKRE4TiL3ktfra6X1a+P30lk3MoksV3xZlx"
          + "cFB4sA2oL36LviU9LnKiqtLlOavWcCBPuqp6Pfnq2fwMyS4+6YPQUCuGglfC+ZPpWmJFnEdJuK8xnmQ6"
          + "uyItvDWFwpKmawYLwnB7raJwIz/oPh2Tj10JtyYWu8gMpyNxnEK6Yi/0Ej/jTsXFbHija7eoR8hoZJuN"
          + "kj6vXaDBlIsn58ps4QqvINrVJ0BNXF7zkB7w+nbK1GgJ43t7l9Vsp19C9mDk4kHN6mjwoNzP4YPAgKOO"
          + "+32CJzFL6vjff/XHKhwuCAAQbGoSDOXEfHN4T/5XB2IgZmtXmjco0c4EzGwXDgRkos+iC8AAbJSXclQL"
          + "3GMrXxu307hX7jV4L1thL8Lp/9ti7RQYBda4EtVyJQ420LjjnblxcOJzk6y9C53niCBNJu5dIQwSaA99"
          + "kvSziJBx6NAkQZ8SXLEmZFYwwlmKSO6cuWoQHSokqqdgkjCshTFYMDMGCSqGSIb3DQEJFDEmHiQAbgBv"
          + "AGQAZQAtADIALgBlAHgAYQBtAHAAbABlAC4AYwBvAG0wIQYJKoZIhvcNAQkVMRQEElRpbWUgMTU5MDYy"
          + "MTM1MTA1NjCCBIQGCSqGSIb3DQEHBqCCBHUwggRxAgEAMIIEagYJKoZIhvcNAQcBMCkGCiqGSIb3DQEM"
          + "AQYwGwQUtc89vOfes93cDoQgMC/PqLfI1r0CAwDDUICCBDBkW0Rs1LzG3L2aK2qBhDICbVWCkrXjtRnF"
          + "TPk9pcLI1vGrXwVp7FRLPxPk1Rfs3haG8jxux2J0mrW0UKbcfjGEdMSX0KcB7vid1kCVL/4jiIrI9DDs"
          + "KGOpxQN5Z+8AoYE4CgVNN0m/2Mo8tJ7jIxuIuiM21VBNLobzUSxKMTdGixzVfMe+Eky4F3mtcgVr7um5"
          + "sim+KeOIfmaQ+Qp1AevIUPca771ozLdxdEH9k5gzOfZG84kcRKxFILxddPJjIXrxdIZayywAOG90tIea"
          + "fhVwPSA7C7FKuV9YlM5cRB1Qr986YCl5qnQyH09sltRWRgf2EctKOwPWvEwWF/ZUuTH/IWQlzJSPd/mG"
          + "WiSsEAWQTG6u4pnbZ3v2vgnikumELbJgvivESn/p/J49UYbfQ/atDQdjEUxx6YC2zhmFJseCkwUzFDd8"
          + "KOxL3cvqLu3ubjFn9a3NBZa42cqQpwvAxTg8rKnc9YzxgF1JZ1PGn7FSRxXVfoqaq5paVZped5cbEMrr"
          + "klj2HcswyRIVYDfsg0mY31myjalf6WPwbQpYScGl2kdgqE2Xm6TrwrpYWG2HZBhaIm2RbD7fUlY5ZSUJ"
          + "D0S3sxjsfCyMQ42kDejtYrOSDR+aRUvWPj1RhaZ1YGnZV4kI/Tyndqb1/X7v1afCvzAHz9EtJpVtecX3"
          + "GylTGPhdZgH4qTc6j2I9X6dT2JHb7do+Wti+FtjzFzcZ00RRf71HZjpxGsV+P8wxBjnOTiY0o8T0TUAi"
          + "/VxCYtbpaP0AvnwzccqaUlH3PQiiX4XJ9NLemW4AzGv2P3lJLcSCACRsK2sRKjVGxPfgsFLEi6QlA0m6"
          + "DejePWfMWi1/dFHk/86rlmd0pF2/QN9GdzKNlWsHuybgEXt9bCrnSA6bDkjzlS0ayn5UFemuJ6zgxdpy"
          + "3DMnEtdSkxPvmB55hiFxErfZAWyzQP2TlyGlTGh/DEiycq3/iy/x2r0svQmW2mPAF6mz7KP5n+Cg1DjJ"
          + "4VteZ2cTBgzOMPQqTDLf7fcLcA7S7lcRS6MJ6q6HcUlIdBfXJu9oNwD4dAK1K1nqx3yxIAYgK7TyuKDx"
          + "0V6yWysB6OdKB5TEdHtK+50ZKEIKvtP+PwDjf9dtbAvH7fFz5lETECoNEx5hJUcdUPaqizf69l2EBUpU"
          + "uzFm906BxozfUMcGc+A3grASXo5KJSfR1dxldeKV4lITFL809z1nxMokwv6f6c4k7/zw9+iUEwFYxARr"
          + "xTgXsLRgzAwHuY1XvWvYZKh/5cgAu8AsSUfT3HjOgOjM/DyXiPKH3IG5gbxZUx5NhlbWA5GO0Sdmy2VG"
          + "wCMLllfHG7FBrYOI5/UnDC6vKZz0GVWqP5Af68UFgj4rILA5btOk1TWo3rWUYGx34pUsONlTkoSDqtnt"
          + "64ybuaOz6BjAhBWmooNhZ5CvdPCqAJczlt7qMD4wITAJBgUrDgMCGgUABBT7RD99hC1dIBi5SyjpcIRK"
          + "uvsHRQQUKmpPPBoczW9SZfPofKeMAp7EAscCAwGGoA==";

  private static final String KEY_PASSWORD = "password";
  private static final String KEYSTORE_PASSWORD = "password";
  private static final String TRUSTSTORE_PASSWORD = "password";

  private static final AtomicReference<Path> keyStorePathNode1 = new AtomicReference<>();
  private static final AtomicReference<Path> keyStorePathNode2 = new AtomicReference<>();

  private MultiNodeKeyStore() {
  }

  /**
   * @return props brokers will need to connect to support SSL connections.
   */
  public static Map<String, String> keyStoreNode1Props() {
    return ImmutableMap.of(
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStorePathNode1(),
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, KEYSTORE_PASSWORD,
        SslConfigs.SSL_KEY_PASSWORD_CONFIG, KEY_PASSWORD,
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, keyStorePathNode1(),
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TRUSTSTORE_PASSWORD
    );
  }

  private static String keyStorePathNode1() {
    final Path path = keyStorePathNode1.updateAndGet(existing -> {
      if (existing != null) {
        return existing;
      }

      return KeyStoreUtil.createTemporaryStore("server-key-store-node1",
          BASE64_ENCODED_STORE_NODE1);
    });

    return path.toAbsolutePath().toString();
  }

  /**
   * @return props brokers will need to connect to support SSL connections.
   */
  public static Map<String, String> keyStoreNode2Props() {
    return ImmutableMap.of(
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStorePathNode2(),
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, KEYSTORE_PASSWORD,
        SslConfigs.SSL_KEY_PASSWORD_CONFIG, KEY_PASSWORD,
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, keyStorePathNode2(),
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TRUSTSTORE_PASSWORD
    );
  }

  private static String keyStorePathNode2() {
    final Path path = keyStorePathNode2.updateAndGet(existing -> {
      if (existing != null) {
        return existing;
      }

      return KeyStoreUtil.createTemporaryStore("server-key-store-node2",
          BASE64_ENCODED_STORE_NODE2);
    });

    return path.toAbsolutePath().toString();
  }
}
