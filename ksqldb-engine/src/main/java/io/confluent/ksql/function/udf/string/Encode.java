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

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
@UdfDescription(name = "encode",
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Takes an input string s, which is encoded as input_encoding, "
        + "and encodes it as output_encoding. The accepted input and output encodings are: "
        + "hex, utf8, ascii and base64. Throws exception if provided encodings are not supported.")
public class Encode {

  static Map<String, Encoder> encoderMap = new HashMap<>();

  static {
    encoderMap.put("hexascii", new HexToAscii());
    encoderMap.put("hexutf8", new HexToUtf8());
    encoderMap.put("hexbase64", new HexToBase64());
    encoderMap.put("utf8ascii", new Utf8ToAscii());
    encoderMap.put("utf8hex", new Utf8ToHex());
    encoderMap.put("utf8base64", new Utf8ToBase64());
    encoderMap.put("asciiutf8", new AsciiToUtf8());
    encoderMap.put("asciihex", new AsciiToHex());
    encoderMap.put("asciibase64", new AsciiToBase64());
    encoderMap.put("base64ascii", new Base64ToAscii());
    encoderMap.put("base64utf8", new Base64ToUtf8());
    encoderMap.put("base64hex", new Base64ToHex());
  }

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

    String encoderString = inputEncoding.toLowerCase() + outputEncoding.toLowerCase();

    if (encoderMap.get(encoderString) == null) {
      throw new KsqlFunctionException("Supported input and output encodings are: "
                                  + "hex, utf8, ascii and base64");
    }
    return encoderMap.get(encoderString).apply(str);
  }


  interface Encoder {
    String apply(String input) throws KsqlFunctionException;
  }

  static class HexToAscii implements Encoder {

    @Override
    public String apply(final String input) {
      try {
        byte[] decoded = Hex.decodeHex(input);
        return new String(decoded, StandardCharsets.US_ASCII);
      } catch (DecoderException e) {
        throw new KsqlFunctionException(e.getMessage());
      }
    }
  }

  static class HexToBase64 implements Encoder {

    @Override
    public String apply(final String input) throws KsqlFunctionException {
      byte[] decodedHex;
      try {
        decodedHex = Hex.decodeHex(input);
      } catch (DecoderException e) {
        throw new KsqlFunctionException(e.getMessage());
      }
      byte[] encodedHexB64 = Base64.encodeBase64(decodedHex);
      return new String(encodedHexB64);

    }
  }

  static class HexToUtf8 implements Encoder {

    @Override
    public String apply(final String input) throws KsqlFunctionException {
      byte[] decodedHex;
      try {
        decodedHex = Hex.decodeHex(input);
      } catch (DecoderException e) {
        throw new KsqlFunctionException(e.getMessage());
      }
      return new String(decodedHex, StandardCharsets.UTF_8);
    }
  }

  static class AsciiToHex implements Encoder {

    @Override
    public String apply(final String input) {
      String hex = Hex.encodeHexString(input.getBytes(StandardCharsets.US_ASCII));
      return hex;
    }
  }

  static class AsciiToBase64 implements Encoder {

    @Override
    public String apply(final String input) {
      byte[] encodedB64 = Base64.encodeBase64(input.getBytes(StandardCharsets.US_ASCII));
      return new String(encodedB64);
    }
  }

  static class AsciiToUtf8 implements Encoder {

    @Override
    public String apply(final String input) {
      byte[] decoded = input.getBytes(StandardCharsets.US_ASCII);
      return new String(decoded, StandardCharsets.UTF_8);
    }
  }

  static class Utf8ToAscii implements Encoder {

    @Override
    public String apply(final String input) {
      byte[] decoded = input.getBytes(StandardCharsets.UTF_8);
      return new String(decoded, StandardCharsets.US_ASCII);
    }
  }

  static class Utf8ToHex implements Encoder {

    @Override
    public String apply(final String input) {
      char[] encodeHex = Hex.encodeHex(input.getBytes(StandardCharsets.UTF_8));
      return new String(encodeHex);
    }
  }

  static class Utf8ToBase64 implements Encoder {

    @Override
    public String apply(final String input) {
      byte[] encodedB64 = Base64.encodeBase64(input.getBytes(StandardCharsets.UTF_8));
      return new String(encodedB64);
    }
  }

  static class Base64ToHex implements Encoder {

    @Override
    public String apply(final String input) throws KsqlFunctionException {
      byte[] decodedB64 = Base64.decodeBase64(input);
      char[] encodedHex = Hex.encodeHex(decodedB64);
      return new String(encodedHex);
    }
  }

  static class Base64ToUtf8 implements Encoder {

    @Override
    public String apply(final String input) throws KsqlFunctionException {
      byte[] decodedB64 = Base64.decodeBase64(input);
      return new String(decodedB64, StandardCharsets.UTF_8);
    }
  }

  static class Base64ToAscii implements Encoder {

    @Override
    public String apply(final String input) throws KsqlFunctionException {
      byte[] decodedB64 = Base64.decodeBase64(input);
      return new String(decodedB64, StandardCharsets.US_ASCII);
    }
  }

  /**
   * Converts a hex string to ASCII.
   * Parses each hex byte to decimal and then to ASCII character.
   */
  /*private String encodeHexToAscii(String hex) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < hex.length(); i += 2) {
      String str = hex.substring(i, i + 2);
      builder.append((char)Integer.parseInt(str, 16));
    }
    return builder.toString();
  }

  private String encodeHexToBase64(String hex) throws DecoderException {
    byte[] decodedHex = Hex.decodeHex(hex);
    byte[] encodedHexB64 = Base64.encodeBase64(decodedHex);
    return new String(encodedHexB64);
  }

  private String encodeHexToUtf8(String hex) throws DecoderException {
    byte[] decodedHex = Hex.decodeHex(hex);
    return new String(decodedHex, StandardCharsets.UTF_8);
  }*/

  /*private String encodeAsciiToHex(String ascii) {
    char[] ch = ascii.toCharArray();

    StringBuilder builder = new StringBuilder();
    for (char c : ch) {
      String hexCode = String.format("%H", c);
      builder.append(hexCode);
    }
    return builder.toString();
  }

  private String encodeAsciiToBase64(String ascii) {
    byte[] encodedB64 = Base64.encodeBase64(ascii.getBytes());
    return new String(encodedB64);
  }

  private String encodeAsciiToUtf8(String ascii) {
    byte[] decoded = ascii.getBytes(StandardCharsets.UTF_8);
    return new String(decoded);
  }*/

  /*private String encodeUtf8ToHex(String utf8) {
    char[] encodeHex = Hex.encodeHex(utf8.getBytes(StandardCharsets.UTF_8));
    return new String(encodeHex);
  }

  private String encodeUtf8ToBase64(String utf8) {
    byte[] encodedB64 = Base64.encodeBase64(utf8.getBytes(StandardCharsets.UTF_8));
    return new String(encodedB64);
  }

  private String encodeUtf8ToAscii(String utf8) {
    byte[] decoded = utf8.getBytes(StandardCharsets.UTF_8);
    return new String(decoded, StandardCharsets.US_ASCII);
  }*/

  /*private String encodeBase64ToHex(String base64) {
    byte[] decodedB64 = Base64.decodeBase64(base64);
    char[] encodedHex = Hex.encodeHex(decodedB64);
    return new String(encodedHex);
  }

  private String encodeBase64ToUtf8(String base64) {
    byte[] decodedB64 = Base64.decodeBase64(base64);
    return new String(decodedB64, StandardCharsets.UTF_8);
  }

  private String encodeBase64ToAscii(String base64) {
    byte[] decodedB64 = Base64.decodeBase64(base64);
    return new String(decodedB64, StandardCharsets.US_ASCII);
  }*/
}
