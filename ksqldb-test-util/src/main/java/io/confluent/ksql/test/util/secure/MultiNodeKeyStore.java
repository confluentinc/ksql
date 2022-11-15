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
  // Keystore containing cert/key for node-1.example.com and node-2.example.com under aliases of
  // the same name.
  private static final String BASE64_ENCODED_STORE =
      "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"
          + "RG28Kr/df87mNo9qmAQcpgIDAYag";

  private static final String KEY_PASSWORD = "password";
  private static final String KEYSTORE_PASSWORD = "password";
  private static final String TRUSTSTORE_PASSWORD = "password";

  private static final AtomicReference<Path> keyStorePathNode = new AtomicReference<>();

  private MultiNodeKeyStore() {
  }

  /**
   * @return props brokers will need to connect to support SSL connections.
   */
  public static Map<String, String> keyStoreProps() {
    return ImmutableMap.of(
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStorePathNode1(),
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, KEYSTORE_PASSWORD,
        SslConfigs.SSL_KEY_PASSWORD_CONFIG, KEY_PASSWORD,
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, keyStorePathNode1(),
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TRUSTSTORE_PASSWORD
    );
  }

  private static String keyStorePathNode1() {
    final Path path = keyStorePathNode.updateAndGet(existing -> {
      if (existing != null) {
        return existing;
      }

      return KeyStoreUtil.createTemporaryStore("server-key-store",
          BASE64_ENCODED_STORE);
    });

    return path.toAbsolutePath().toString();
  }
}
