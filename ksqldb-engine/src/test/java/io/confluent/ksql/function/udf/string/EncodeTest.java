/*
 * Copyright 2019 Confluent Inc.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.confluent.ksql.function.KsqlFunctionException;
import org.junit.Assert;
import org.junit.Test;

public class EncodeTest {

  private Encode udf = new Encode();

  @Test
  public void shouldReturnNullOnNullValue() {
    assertThat(udf.encode(null, "hex", "ascii"), is(nullValue()));
    assertThat(udf.encode(null, "utf8", "base64"), is(nullValue()));
    assertThat(udf.encode("some string", null, "utf8"), is(nullValue()));
    assertThat(udf.encode("some string", "hex", null), is(nullValue()));
  }

  @Test
  public void shouldEncodeHexToAscii() {
    assertThat(udf.encode("4578616d706C6521", "hex", "ascii"), is("Example!"));
    assertThat(udf.encode("506C616E74207472656573", "hex", "ascii"), is("Plant trees"));
    assertThat(udf.encode("31202b2031203d2031", "hex", "ascii"), is("1 + 1 = 1"));
    assertThat(udf.encode("ce95cebbcebbceacceb4ceb1", "hex", "ascii"), is("������������"));
    assertThat(udf.encode("c39c6265726d656e736368", "hex", "ascii"), is("��bermensch"));

    assertThat(udf.encode("0x48656c6c6f20576f726c6421", "hex", "ascii"), is("Hello World!"));
    assertThat(udf.encode("0x9", "hex", "ascii"), is("\t"));
    assertThat(udf.encode("0x", "hex", "ascii"), is(""));
    assertThat(udf.encode("X'436c6f7564792a7e2a3f'", "hex", "ascii"), is("Cloudy*~*?"));
    assertThat(udf.encode("x'4578616d706C6521'", "hex", "ascii"), is("Example!"));

    assertThat(udf.encode("X''", "hex", "ascii"), is(""));
    assertThat(udf.encode("x''", "hex", "ascii"), is(""));
    assertThat(udf.encode("0x578616d706C6521", "hex", "ascii"), is("\u0005xample!"));
    Assert.assertThrows(KsqlFunctionException.class, () -> udf.encode("578616d706C6521", "hex", "ascii"));
    Assert.assertThrows(KsqlFunctionException.class, () -> udf.encode("X'578616d706C6521'", "hex", "ascii"));
    Assert.assertThrows(KsqlFunctionException.class, () -> udf.encode("x'578616d706C6521'", "hex", "ascii"));

  }

  @Test
  public void shouldEncodeHexToUtf8() {
    assertThat(udf.encode("4578616d706c6521", "hex", "utf8"), is("Example!"));
    assertThat(udf.encode("506c616e74207472656573", "hex", "utf8"), is("Plant trees"));
    assertThat(udf.encode("31202b2031203d2031", "hex", "utf8"), is("1 + 1 = 1"));
    assertThat(udf.encode("ce95cebbcebbceacceb4ceb1", "hex", "utf8"), is("Ελλάδα"));
    assertThat(udf.encode("c39c6265726d656e736368", "hex", "utf8"), is("Übermensch"));

    assertThat(udf.encode("0x4578616d706c6521", "hex", "utf8"), is("Example!"));
    assertThat(udf.encode("0x", "hex", "utf8"), is(""));
    assertThat(udf.encode("X'506C6174666F726D2D7C5F5F5F5F5F7C2D'", "hex", "utf8"), is("Platform-|_____|-"));
    assertThat(udf.encode("x'31202b2031203d2031'", "hex", "utf8"), is("1 + 1 = 1"));

    assertThat(udf.encode("X''", "hex", "utf8"), is(""));
    assertThat(udf.encode("x''", "hex", "utf8"), is(""));
  }

  @Test
  public void shouldEncodeHexToBase64() {
    assertThat(udf.encode("4578616d706c6521", "hex", "base64"), is("RXhhbXBsZSE="));
    assertThat(udf.encode("506c616e74207472656573", "hex", "base64"), is("UGxhbnQgdHJlZXM="));
    assertThat(udf.encode("31202b2031203d2031", "hex", "base64"), is("MSArIDEgPSAx"));
    assertThat(udf.encode("ce95cebbcebbceacceb4ceb1", "hex", "base64"), is("zpXOu867zqzOtM6x"));
    assertThat(udf.encode("c39c6265726d656e736368", "hex", "base64"), is("w5xiZXJtZW5zY2g="));

    assertThat(udf.encode("0x4578616d706c6521", "hex", "base64"), is("RXhhbXBsZSE="));
    assertThat(udf.encode("X'7e8a016abfff'", "hex", "base64"), is("fooBar//"));
    assertThat(udf.encode("x'328ba7b5a8a75627b0'", "hex", "base64"), is("MountainView"));
    assertThat(udf.encode("0x", "hex", "base64"), is(""));
    assertThat(udf.encode("X''", "hex", "base64"), is(""));
    assertThat(udf.encode("x''", "hex", "base64"), is(""));
  }

  @Test
  public void shouldEncodeAsciiToHex() {
    assertThat(udf.encode("Example!", "ascii", "hex"), is("4578616d706c6521"));
    assertThat(udf.encode("Plant trees", "ascii", "hex"), is("506c616e74207472656573"));
    assertThat(udf.encode("1 + 1 = 1", "ascii", "hex"), is("31202b2031203d2031"));
    assertThat(udf.encode("Ελλάδα", "ascii", "hex"), is("3f3f3f3f3f3f"));
    assertThat(udf.encode("Übermensch", "ascii", "hex"), is("3f6265726d656e736368"));
  }

  @Test
  public void shouldEncodeAsciiToUtf8() {
    assertThat(udf.encode("Example!", "ascii", "utf8"), is("Example!"));
    assertThat(udf.encode("Plant trees", "ascii", "utf8"), is("Plant trees"));
    assertThat(udf.encode("1 + 1 = 1", "ascii", "utf8"), is("1 + 1 = 1"));
    assertThat(udf.encode("Ελλάδα", "ascii", "utf8"), is("??????"));
    assertThat(udf.encode("Übermensch", "ascii", "utf8"), is("?bermensch"));
  }

  @Test
  public void shouldEncodeAsciiToBase64() {
    assertThat(udf.encode("Example!", "ascii", "base64"), is("RXhhbXBsZSE="));
    assertThat(udf.encode("Plant trees", "ascii", "base64"), is("UGxhbnQgdHJlZXM="));
    assertThat(udf.encode("1 + 1 = 1", "ascii", "base64"), is("MSArIDEgPSAx"));
    assertThat(udf.encode("Ελλάδα", "ascii", "base64"), is("Pz8/Pz8/"));
    assertThat(udf.encode("Übermensch", "ascii", "base64"), is("P2Jlcm1lbnNjaA=="));
  }

  @Test
  public void shouldEncodeUtf8ToHex() {
    assertThat(udf.encode("Example!", "utf8", "hex"), is("4578616d706c6521"));
    assertThat(udf.encode("Plant trees", "utf8", "hex"), is("506c616e74207472656573"));
    assertThat(udf.encode("1 + 1 = 1", "utf8", "hex"), is("31202b2031203d2031"));
    assertThat(udf.encode("Ελλάδα", "utf8", "hex"), is("ce95cebbcebbceacceb4ceb1"));
    assertThat(udf.encode("Übermensch", "utf8", "hex"), is("c39c6265726d656e736368"));
  }

  @Test
  public void shouldEncodeUtf8ToAscii() {
    assertThat(udf.encode("Example!", "utf8", "ascii"), is("Example!"));
    assertThat(udf.encode("Plant trees", "utf8", "ascii"), is("Plant trees"));
    assertThat(udf.encode("1 + 1 = 1", "utf8", "ascii"), is("1 + 1 = 1"));
    assertThat(udf.encode("Ελλάδα", "utf8", "ascii"), is("������������"));
    assertThat(udf.encode("Übermensch", "utf8", "ascii"), is("��bermensch"));
  }

  @Test
  public void shouldEncodeUtf8ToBase64() {
    assertThat(udf.encode("Example!", "utf8", "base64"), is("RXhhbXBsZSE="));
    assertThat(udf.encode("Plant trees", "utf8", "base64"), is("UGxhbnQgdHJlZXM="));
    assertThat(udf.encode("1 + 1 = 1", "utf8", "base64"), is("MSArIDEgPSAx"));
    assertThat(udf.encode("Ελλάδα", "utf8", "base64"), is("zpXOu867zqzOtM6x"));
    assertThat(udf.encode("Übermensch", "utf8", "base64"), is("w5xiZXJtZW5zY2g="));
  }

  @Test
  public void shouldEncodeBase64ToUtf8() {
    assertThat(udf.encode("RXhhbXBsZSE=", "base64", "utf8"), is("Example!"));
    assertThat(udf.encode("UGxhbnQgdHJlZXM=", "base64", "utf8"), is("Plant trees"));
    assertThat(udf.encode("MSArIDEgPSAx", "base64", "utf8"), is("1 + 1 = 1"));
    assertThat(udf.encode("zpXOu867zqzOtM6x", "base64", "utf8"), is("Ελλάδα"));
    assertThat(udf.encode("w5xiZXJtZW5zY2g", "base64", "utf8"), is("Übermensch"));
  }

  @Test
  public void shouldEncodeBase64ToHex() {
    assertThat(udf.encode("RXhhbXBsZSE=", "base64", "hex"), is("4578616d706c6521"));
    assertThat(udf.encode("UGxhbnQgdHJlZXM=", "base64", "hex"), is("506c616e74207472656573"));
    assertThat(udf.encode("MSArIDEgPSAx", "base64", "hex"), is("31202b2031203d2031"));
    assertThat(udf.encode("zpXOu867zqzOtM6x", "base64", "hex"), is("ce95cebbcebbceacceb4ceb1"));
    assertThat(udf.encode("w5xiZXJtZW5zY2g", "base64", "hex"), is("c39c6265726d656e736368"));
  }

  @Test
  public void shouldEncodeBase64ToAscii() {
    assertThat(udf.encode("RXhhbXBsZSE=", "base64", "ascii"), is("Example!"));
    assertThat(udf.encode("UGxhbnQgdHJlZXM=", "base64", "utf8"), is("Plant trees"));
    assertThat(udf.encode("MSArIDEgPSAx", "base64", "ascii"), is("1 + 1 = 1"));
    assertThat(udf.encode("zpXOu867zqzOtM6x", "base64", "ascii"), is("������������"));
    assertThat(udf.encode("w5xiZXJtZW5zY2g", "base64", "ascii"), is("��bermensch"));
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowIfUnsupportedEncodingTypes() {
    udf.encode("4578616d706C6521", "hex", "hex");
    udf.encode("Ελλάδα", "utf8", "utf8");
    udf.encode("1 + 1 = 1", "ascii", "ascii");
    udf.encode("w5xiZXJtZW5zY2g=", "base64", "base64");
  }
}
