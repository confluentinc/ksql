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

package io.confluent.ksql.function.udf.string;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.nio.charset.StandardCharsets;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
@UdfDescription(name = "encode",
    author = KsqlConstants.CONFLUENT_AUTHOR,
    category = FunctionCategory.STRING,
    description = "Takes an input string s, which is encoded as input_encoding, "
        + "and encodes it as output_encoding. The accepted input and output encodings are: "
        + "hex, utf8, ascii and base64. Throws exception if provided encodings are not supported.")
public class Encode {

  private static ImmutableMap<String, Encoder> ENCODER_MAP =
      new ImmutableMap.Builder<String, Encoder>()
      .put("hexascii", new HexToAscii())
      .put("hexutf8", new HexToUtf8())
      .put("hexbase64", new HexToBase64())
      .put("utf8ascii", new Utf8ToAscii())
      .put("utf8hex", new Utf8ToHex())
      .put("utf8base64", new Utf8ToBase64())
      .put("asciiutf8", new AsciiToUtf8())
      .put("asciihex", new AsciiToHex())
      .put("asciibase64", new AsciiToBase64())
      .put("base64ascii", new Base64ToAscii())
      .put("base64utf8", new Base64ToUtf8())
      .put("base64hex", new Base64ToHex())
      .build();

  @Udf(description = "Returns a new string encoded using the outputEncoding ")
  public String encode(
      @UdfParameter(
          description = "The source string. If null, then function returns null.") final String str,
      @UdfParameter(
          description = "The input encoding."
              + " If null, then function returns null.") final String inputEncoding,
      @UdfParameter(
          description = "The output encoding."
              + " If null, then function returns null.") final String outputEncoding) {
    if (str == null || inputEncoding == null || outputEncoding == null) {
      return null;
    }

    final String encodedString = inputEncoding.toLowerCase() + outputEncoding.toLowerCase();

    if (ENCODER_MAP.get(encodedString) == null) {
      throw new KsqlFunctionException("Supported input and output encodings are: "
                                  + "hex, utf8, ascii and base64");
    }
    return ENCODER_MAP.get(encodedString).apply(str);
  }

  interface Encoder {
    String apply(String input) throws KsqlFunctionException;
  }

  static class HexToAscii implements Encoder {

    @Override
    public String apply(final String input) {
      try {
        //strip away "Ox" from front or "X\'" + "\'" from front or back of hex if present
        final String processedInput;
        processedInput = hexStrip(input);

        final byte[] decoded = Hex.decodeHex(processedInput);
        return new String(decoded, StandardCharsets.US_ASCII);
      } catch (DecoderException e) {
        throw new KsqlFunctionException(e.getMessage());
      }
    }
  }

  static class HexToBase64 implements Encoder {

    @Override
    public String apply(final String input) throws KsqlFunctionException {
      final byte[] decodedHex;
      //strip away "Ox" from front or "X\'" + "\'" from front and back of hex if present
      final String processedInput;
      processedInput = hexStrip(input);
      try {
        decodedHex = Hex.decodeHex(processedInput);
      } catch (DecoderException e) {
        throw new KsqlFunctionException(e.getMessage());
      }
      final byte[] encodedB64 = Base64.encodeBase64(decodedHex);
      return new String(encodedB64, StandardCharsets.UTF_8);

    }
  }

  static class HexToUtf8 implements Encoder {

    @Override
    public String apply(final String input) throws KsqlFunctionException {
      final byte[] decodedHex;
      //strip away "Ox" from front or "X\'" + "\'" from front and back of hex if present
      final String processedInput;
      processedInput = hexStrip(input);
      try {
        decodedHex = Hex.decodeHex(processedInput);
      } catch (DecoderException e) {
        throw new KsqlFunctionException(e.getMessage());
      }
      return new String(decodedHex, StandardCharsets.UTF_8);
    }
  }

  static class AsciiToHex implements Encoder {

    @Override
    public String apply(final String input) {
      return Hex.encodeHexString(input.getBytes(StandardCharsets.US_ASCII));
    }
  }

  static class AsciiToBase64 implements Encoder {

    @Override
    public String apply(final String input) {
      final byte[] encodedB64 = Base64.encodeBase64(input.getBytes(StandardCharsets.US_ASCII));
      return new String(encodedB64, StandardCharsets.UTF_8);
    }
  }

  static class AsciiToUtf8 implements Encoder {

    @Override
    public String apply(final String input) {
      final byte[] decoded = input.getBytes(StandardCharsets.US_ASCII);
      return new String(decoded, StandardCharsets.UTF_8);
    }
  }

  static class Utf8ToAscii implements Encoder {

    @Override
    public String apply(final String input) {
      final byte[] decoded = input.getBytes(StandardCharsets.UTF_8);
      return new String(decoded, StandardCharsets.US_ASCII);
    }
  }

  static class Utf8ToHex implements Encoder {

    @Override
    public String apply(final String input) {
      final char[] encodeHex = Hex.encodeHex(input.getBytes(StandardCharsets.UTF_8));
      return new String(encodeHex);
    }
  }

  static class Utf8ToBase64 implements Encoder {

    @Override
    public String apply(final String input) {
      final byte[] encodedB64 = Base64.encodeBase64(input.getBytes(StandardCharsets.UTF_8));
      return new String(encodedB64, StandardCharsets.UTF_8);
    }
  }

  static class Base64ToHex implements Encoder {

    @Override
    public String apply(final String input) throws KsqlFunctionException {
      final byte[] decodedB64 = Base64.decodeBase64(input);
      final char[] encodedHex = Hex.encodeHex(decodedB64);
      return new String(encodedHex);
    }
  }

  static class Base64ToUtf8 implements Encoder {

    @Override
    public String apply(final String input) throws KsqlFunctionException {
      final byte[] decodedB64 = Base64.decodeBase64(input);
      return new String(decodedB64, StandardCharsets.UTF_8);
    }
  }

  static class Base64ToAscii implements Encoder {

    @Override
    public String apply(final String input) throws KsqlFunctionException {
      final byte[] decodedB64 = Base64.decodeBase64(input);
      return new String(decodedB64, StandardCharsets.US_ASCII);
    }
  }

  /**
   Strips away the "0x" from hex of type "0xAB79" and
   strips away the "X\'" from front and "\'" from end of hex of type "X'AB79'".
   Leaves every other type of hex (like AB79) untouched

   @param hexString unstripped hex String
   @return the string after removing
   */
  public static String hexStrip(final String hexString) {
    final int hexLen = hexString.length();

    if (hexString.matches("0x.*")) {
      //matches with things like "0x" and "0x...."

      //add an extra "0" to the front if there are odd number of digits
      return hexLen % 2 != 0 ? "0" + hexString.substring(2) : hexString.substring(2);
    } else if (hexString.matches("(x|X)\'.*\'")) {
      //matches with things like "x''", "X''", "x'....'" and "X'....'"
      return hexString.substring(2, hexLen - 1);
    } else {
      return hexString;
    }
  }
}
