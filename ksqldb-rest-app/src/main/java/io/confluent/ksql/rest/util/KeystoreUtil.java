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

public final class KeystoreUtil {
  private static final String KEYSTORE_TYPE = "JKS";

  private KeystoreUtil() {}

  /**
   * Utility to fetch a Vert.x Buffer that is the serialized version of the key store from the
   * given path, but which contains only the entry for the given alias.  This circumvents Vert.x's
   * direct support of an alias option.
   * @param keyStorePath The original key store which may contain multiple certificates
   * @param password The key store password
   * @param alias The alias of the entry to extract
   * @return The Buffer containing the keystore
   */
  public static Buffer getKeyStore(
      final String keyStorePath,
      final String password,
      final String alias
  ) {
    final char[] pw = password != null ? password.toCharArray() : null;
    final KeyStore keyStore = loadExistingKeyStore(keyStorePath, pw);

    final PrivateKey key;
    final Certificate[] chain;
    try {
      key = (PrivateKey) keyStore.getKey(alias, pw);
      chain = keyStore.getCertificateChain(alias);
    } catch (Exception e) {
      throw new KsqlException("Error fetching key/certificate", e);
    }

    if (key == null || chain == null) {
      throw new KsqlException("Alias doesn't exist in keystore: " + alias);
    }

    final byte[] singleValueKeyStore = createSingleValueKeyStore(key, chain, pw, alias);
    return Buffer.buffer(singleValueKeyStore);
  }

  private static KeyStore loadExistingKeyStore(final String keyStorePath, final char[] pw) {
    try {
      final KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
      keyStore.load(new FileInputStream(keyStorePath), pw);
      return keyStore;
    } catch (Exception e) {
      throw new KsqlException("Couldn't fetch keystore", e);
    }
  }

  private static byte[] createSingleValueKeyStore(
      final PrivateKey key, final Certificate[] chain, final char[] pw, final String alias) {
    try {
      final KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
      keyStore.load(null, null);
      keyStore.setEntry(alias, new KeyStore.PrivateKeyEntry(key, chain),
          new KeyStore.PasswordProtection(pw));
      final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      keyStore.store(outputStream, pw);
      return outputStream.toByteArray();
    } catch (Exception e) {
      throw new KsqlException("Couldn't create keystore", e);
    }
  }
}
