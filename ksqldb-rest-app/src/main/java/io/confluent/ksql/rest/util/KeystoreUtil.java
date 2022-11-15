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

package io.confluent.ksql.rest.util;

import io.confluent.ksql.util.KsqlException;
import io.vertx.core.buffer.Buffer;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KeystoreUtil {
  private static final Logger LOG = LoggerFactory.getLogger(KeystoreUtil.class);
  private static final String KEYSTORE_TYPE = "JKS";

  private KeystoreUtil() {}

  /**
   * Utility to fetch a Vert.x Buffer that is the serialized version of the key store from the
   * given path, but which contains only the entry for the given alias.  This circumvents Vert.x's
   * lack of direct support of an alias option.
   * @param keyStorePath The original key store which may contain multiple certificates
   * @param keyStorePassword The key store password
   * @param alias The alias of the entry to extract
   * @return The Buffer containing the keystore
   */
  public static Buffer getKeyStore(
      final String keyStoreType,
      final String keyStorePath,
      final Optional<String> keyStorePassword,
      final Optional<String> keyPassword,
      final String alias
  ) {
    final char[] pw = keyStorePassword.map(String::toCharArray).orElse(null);
    final char[] keyPw = keyPassword.map(String::toCharArray).orElse(null);
    final KeyStore keyStore = loadExistingKeyStore(keyStoreType, keyStorePath, pw);

    final PrivateKey key;
    final Certificate[] chain;
    try {
      key = (PrivateKey) keyStore.getKey(alias, keyPw);
      chain = keyStore.getCertificateChain(alias);
    } catch (Exception e) {
      throw new KsqlException("Error fetching key/certificate " + alias, e);
    }

    if (key == null || chain == null) {
      throw new KsqlException("Alias doesn't exist in keystore: " + alias);
    }

    final byte[] singleValueKeyStore = createSingleValueKeyStore(key, chain, pw, keyPw, alias);
    return Buffer.buffer(singleValueKeyStore);
  }

  private static KeyStore loadExistingKeyStore(
      final String keyStoreType,
      final String keyStorePath,
      final char[] pw) {
    try (FileInputStream input = new FileInputStream(keyStorePath)) {
      final KeyStore keyStore = KeyStore.getInstance(keyStoreType);
      keyStore.load(input, pw);
      return keyStore;
    } catch (Exception e) {
      throw new KsqlException("Couldn't fetch keystore", e);
    }
  }

  private static byte[] createSingleValueKeyStore(
      final PrivateKey key, final Certificate[] chain, final char[] pw, final char[] keyPw,
      final String alias) {
    try {
      final KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
      keyStore.load(null, null);
      keyStore.setEntry(alias, new KeyStore.PrivateKeyEntry(key, chain),
          new KeyStore.PasswordProtection(keyPw));
      final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      keyStore.store(outputStream, pw);
      return outputStream.toByteArray();
    } catch (Exception e) {
      throw new KsqlException("Couldn't create keystore", e);
    }
  }
}
