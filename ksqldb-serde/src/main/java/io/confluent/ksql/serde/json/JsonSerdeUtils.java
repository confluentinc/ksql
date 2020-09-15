/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.serde.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nonnull;

public final class JsonSerdeUtils {

  // the JsonSchemaConverter adds a magic NULL byte and 4 bytes for the
  // schema ID at the start of the message
  public static final int SIZE_OF_SR_PREFIX = Byte.BYTES + Integer.BYTES;
  public static final int MAGIC_BYTE = 0x00;

  private JsonSerdeUtils() {
  }

  /**
   * Converts {@code jsonWithMagic} into an {@link InputStream} that represents
   * standard JSON encoding.
   *
   * @param jsonWithMagic the serialized JSON
   * @return the corresponding input stream
   * @throws io.confluent.ksql.util.KsqlException If the input is not encoded
   *         using the schema registry format (first byte magic byte, then
   *         four bytes for the schemaID).
   */
  public static <T> T readJsonSR(
      @Nonnull final byte[] jsonWithMagic,
      final ObjectMapper mapper,
      final Class<? extends T> clazz
  ) throws IOException {
    if (!hasMagicByte(jsonWithMagic)) {
      // don't log contents of jsonWithMagic to avoid leaking data into the logs
      throw new KsqlException(
          "Got unexpected JSON serialization format that did not start with the magic byte. If "
              + "this stream was not serialized using the JsonSchemaConverter, then make sure "
              + "the stream is declared with JSON format (not JSON_SR).");
    }

    return mapper.readValue(
        jsonWithMagic,
        SIZE_OF_SR_PREFIX,
        jsonWithMagic.length - SIZE_OF_SR_PREFIX,
        clazz
    );
  }

  /**
   * @param json the serialized JSON
   * @return whether or not this JSON contains the magic schema registry byte
   */
  static boolean hasMagicByte(@Nonnull final byte[] json) {
    // (https://tools.ietf.org/html/rfc7159#section-2) valid JSON should not
    // start with 0x00 - the only "insignificant" characters allowed are
    // 0x20, 0x09, 0x0A and 0x0D
    return json.length > 0 && json[0] == MAGIC_BYTE;
  }

  static boolean toBoolean(final JsonNode object) {
    if (object instanceof BooleanNode) {
      return object.booleanValue();
    }

    throw invalidConversionException(object, SqlBaseType.BOOLEAN);
  }

  static int toInteger(final JsonNode object) {
    if (object instanceof NumericNode) {
      return object.intValue();
    }
    if (object instanceof TextNode) {
      try {
        return Integer.parseInt(object.textValue());
      } catch (final NumberFormatException e) {
        throw failedStringCoercionException(SqlBaseType.INTEGER);
      }
    }
    throw invalidConversionException(object, SqlBaseType.INTEGER);
  }

  static long toLong(final JsonNode object) {
    if (object instanceof NumericNode) {
      return object.asLong();
    }
    if (object instanceof TextNode) {
      try {
        return Long.parseLong(object.textValue());
      } catch (final NumberFormatException e) {
        throw failedStringCoercionException(SqlBaseType.BIGINT);
      }
    }
    throw invalidConversionException(object, SqlBaseType.BIGINT);
  }

  static double toDouble(final JsonNode object) {
    if (object instanceof NumericNode) {
      return object.doubleValue();
    }
    if (object instanceof TextNode) {
      try {
        return Double.parseDouble(object.textValue());
      } catch (final NumberFormatException e) {
        throw failedStringCoercionException(SqlBaseType.DOUBLE);
      }
    }

    throw invalidConversionException(object, SqlBaseType.DOUBLE);
  }

  static IllegalArgumentException invalidConversionException(
      final Object object,
      final String sqlType
  ) {
    return new IllegalArgumentException("Can't convert type."
        + " sourceType: " + object.getClass().getSimpleName()
        + ", requiredType: " + sqlType);
  }

  private static IllegalArgumentException invalidConversionException(
      final Object object,
      final SqlBaseType sqlType
  ) {
    return invalidConversionException(object, sqlType.toString());
  }

  private static IllegalArgumentException failedStringCoercionException(final SqlBaseType sqlType) {
    return new IllegalArgumentException("Can't coerce string to type. targetType: " + sqlType);
  }
}
