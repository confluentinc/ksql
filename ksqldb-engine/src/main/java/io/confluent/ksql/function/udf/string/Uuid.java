/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.ksql.function.udf.string;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

@UdfDescription(
    name = "UUID",
    category = FunctionCategory.STRING,
    description = "Create a Universally Unique Identifier (UUID) generated according to RFC 4122. "
        + "A call to UUID() returns a value conforming to UUID version 4, sometimes called "
        + "\"random UUID\", as described in RFC 4122. A call to UUID(bytes) returns a value "
        + "conforming to UUID. The value is a 128-bit number represented as a string of "
        + "five hexadecimal numbers aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.")
public class Uuid {

  @Udf
  public String uuid() {
    return java.util.UUID.randomUUID().toString();
  }

  @Udf
  public String uuid(@UdfParameter final ByteBuffer bytes) {
    if (bytes == null || bytes.capacity() != 16) {
      return null;
    }

    try {
      final long firstLong = bytes.getLong();
      final long secondLong = bytes.getLong();

      return new java.util.UUID(firstLong, secondLong).toString();
    } catch (BufferUnderflowException ex) {
      return null;
    }
  }
}
