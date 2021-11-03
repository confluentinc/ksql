/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Base64;

public final class OffsetVectorFactory {

  private OffsetVectorFactory() {

  }

  public static OffsetVector createOffsetVector() {
    return new OffsetVectorImpl();
  }

  public static OffsetVector deserialize(final String token) {
    final byte[] data = Base64.getDecoder().decode(token);
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data))) {
      final OffsetVector ct = (OffsetVector) in.readObject();
      return ct;
    } catch (Exception e) {
      throw new KsqlException("Couldn't decode consistency token", e);
    }
  }
}



