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
import io.vertx.core.buffer.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.xml.bind.DatatypeConverter;

public final class BytesUtils {
  public enum Encoding {
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

  public static ByteOrder byteOrderType(final String byteOrderStr) {
    if (byteOrderStr != null
        && byteOrderStr.equalsIgnoreCase(ByteOrder.BIG_ENDIAN.toString())) {
      return ByteOrder.BIG_ENDIAN;
    } else if (byteOrderStr != null
        && byteOrderStr.equalsIgnoreCase(ByteOrder.LITTLE_ENDIAN.toString())) {
      return ByteOrder.LITTLE_ENDIAN;
    } else {
      throw new KsqlException(String.format(
          "Byte order must be BIG_ENDIAN or LITTLE_ENDIAN. Unknown byte order '%s'.",
          byteOrderStr));
    }
  }

  public static String encode(final byte[] value, final Encoding encoding) {
    final Function<byte[], String> encoder = ENCODERS.get(encoding);
    if (encoder == null) {
      throw new IllegalStateException(String.format("Unsupported encoding type '%s'. ", encoding));
    }

    return encoder.apply(value);
  }

  public static byte[] decode(final String value, final Encoding encoding) {
    final Function<String, byte[]> decoder = DECODERS.get(encoding);
    if (decoder == null) {
      throw new IllegalStateException(String.format("Unsupported encoding type '%s'. ", encoding));
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

  public static Buffer toJsonMsg(final Buffer responseLine, final boolean stripArray) {

    int start = 0;
    int end = responseLine.length() - 1;
    if (stripArray) {
      if (responseLine.getByte(0) == (byte) '[') {
        start = 1;
      }
      if (responseLine.getByte(end) == (byte) ']') {
        end -= 1;
      }
    }
    if (end > 0 && responseLine.getByte(end) == (byte) ',') {
      end -= 1;
    }
    return responseLine.slice(start, end + 1);
  }

  public static List<byte[]> split(final byte[] b, final byte[] delim) {
    if (b.length == 0) {
      return Arrays.asList(Arrays.copyOf(b, b.length));
    } else if (delim.length == 0) {
      return splitAllBytes(b);
    }

    int offset = 0;
    int delimIdx;

    final List<byte[]> list = new ArrayList<>();
    while ((delimIdx = indexOf(b, delim, offset)) != -1) {
      final int newSplitLength = delimIdx - offset;
      final byte[] newSplitArray = new byte[newSplitLength];

      System.arraycopy(b, offset, newSplitArray, 0, newSplitLength);
      list.add(newSplitArray);

      offset = delimIdx + delim.length;
    }

    list.add(Arrays.copyOfRange(b, offset, b.length));

    return list;
  }

  private static List<byte[]> splitAllBytes(final byte[] b) {
    final List<byte[]> result = new ArrayList<>(b.length);
    for (int i = 0; i < b.length; i++) {
      result.add(new byte[] { b[i] });
    }

    return result;
  }

  public static int indexOf(final byte[] array, final byte[] target, final int fromIndex) {
    for (int i = fromIndex; i < array.length; i++) {
      if (array[i] == target[0]) {
        if (arrayEquals(array, i, target, 0, target.length)) {
          return i;
        }
      }
    }

    return -1;
  }

  public static void checkBytesSize(final ByteBuffer buffer, final int size) {
    final int bufferSize = getByteArray(buffer).length;
    if (bufferSize != size) {
      throw new KsqlException(
          String.format("Number of bytes must be equal to %d, but found %d", size, bufferSize));
    }
  }

  @SuppressWarnings("ParameterName")
  private static boolean arrayEquals(
      final byte[] a,
      final int aFromIndex,
      final byte[] b,
      final int bFromIndex,
      final int length) {
    if ((aFromIndex + length) > a.length) {
      return false;
    }

    if ((bFromIndex + length) > b.length) {
      return false;
    }

    for (int i = aFromIndex, j = bFromIndex; i < length && j < b.length; i++, j++) {
      if (a[i] != b[i]) {
        return false;
      }
    }

    return true;
  }

  private static String hexEncoding(final byte[] value) {
    return DatatypeConverter.printHexBinary(value);
  }

  private static byte[] hexDecoding(final String value) {
    return DatatypeConverter.parseHexBinary(value);
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
    // Use getEncoder() because it does not add \r\n to the base64 string
    return Base64.getEncoder().encodeToString(value);
  }

  private static byte[] base64Decoding(final String value) {
    return Base64.getDecoder().decode(value);
  }
}
