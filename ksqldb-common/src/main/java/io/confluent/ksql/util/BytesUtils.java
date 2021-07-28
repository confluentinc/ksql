/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.function.Function;

public final class BytesUtils {
  private static final Base64.Encoder BASE64_ENCODER = Base64.getMimeEncoder();
  private static final Base64.Decoder BASE64_DECODER = Base64.getMimeDecoder();

  enum Encoding {
    HEX,
    UTF8,
    ASCII,
    BASE64;

    public static Encoding from(final String value) {
      if (value.equalsIgnoreCase("hex")) {
        return HEX;
      } else if (value.equalsIgnoreCase("utf8")) {
        return UTF8;
      } else if (value.equalsIgnoreCase("ascii")) {
        return ASCII;
      } else if (value.equalsIgnoreCase("base64")) {
        return BASE64;
      }

      throw new IllegalArgumentException("Unknown encoding type '" + value + "'. "
          + "Supported formats are 'hex', 'utf8', 'ascii', and 'base64'.");
    }
  }

  private BytesUtils() {
  }

  private static final Map<Encoding, Function<byte[], String>> ENCODERS = ImmutableMap.of(
      Encoding.HEX, v -> hexEncoding(v),
      Encoding.UTF8, v -> utf8Encoding(v),
      Encoding.ASCII, v -> asciiEncoding(v),
      Encoding.BASE64, v -> base64Encoding(v)
  );

  private static final Map<Encoding, Function<String, byte[]>> DECODERS = ImmutableMap.of(
      Encoding.HEX, v -> hexDecoding(v),
      Encoding.UTF8, v -> utf8Decoding(v),
      Encoding.ASCII, v -> asciiDecoding(v),
      Encoding.BASE64, v -> base64Decoding(v)
  );

  public static String encode(final byte[] value, final String encoding) {
    final Function<byte[], String> encoder = ENCODERS.get(Encoding.from(encoding));
    if (encoder == null) {
      throw new IllegalStateException(String.format("Unknown encoding type '%s'. "
          + "Supported formats are 'hex', 'utf8', 'ascii', and 'base64'.", encoding));
    }

    return encoder.apply(value);
  }

  public static byte[] decode(final String value, final String encoding) {
    final Function<String, byte[]> decoder = DECODERS.get(Encoding.from(encoding));
    if (decoder == null) {
      throw new IllegalStateException(String.format("Unknown encoding type '%s'. "
          + "Supported formats are 'hex', 'utf8', 'ascii', and 'base64'.", encoding));
    }

    return decoder.apply(value);
  }

  public static byte[] getByteArray(final ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }

    // ByteBuffer.array() throws an exception if it is read-only or the array is null.
    // Protobuf returns ByteBuffer as read-only, so this util allows us to get the internal
    // byte array.
    if (!buffer.hasArray()) {
      // Reset internal array position to 0, which affects read-only buffers
      buffer.clear();

      final byte[] internalByteArray = new byte[buffer.capacity()];
      buffer.get(internalByteArray);
      return internalByteArray;
    }

    return buffer.array();
  }

  public static byte[] getByteArray(final ByteBuffer buffer, final int start, final int end) {
    return Arrays.copyOfRange(getByteArray(buffer), start, end);
  }

  private static String hexEncoding(final byte[] value) {
    return BaseEncoding.base16().encode(value);
  }

  private static byte[] hexDecoding(final String value) {
    return BaseEncoding.base16().decode(value);
  }

  private static String utf8Encoding(final byte[] value) {
    return new String(value, StandardCharsets.UTF_8);
  }

  private static byte[] utf8Decoding(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static String asciiEncoding(final byte[] value) {
    return new String(value, StandardCharsets.US_ASCII);
  }

  private static byte[] asciiDecoding(final String value) {
    return value.getBytes(StandardCharsets.US_ASCII);
  }

  private static String base64Encoding(final byte[] value) {
    return BASE64_ENCODER.encodeToString(value);
  }

  private static byte[] base64Decoding(final String value) {
    return BASE64_DECODER.decode(value);
  }
}
