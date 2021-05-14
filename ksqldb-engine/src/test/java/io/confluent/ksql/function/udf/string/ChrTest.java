/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use this file except
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

public class ChrTest {
  private final Chr udf = new Chr();

  @Test
  public void shouldConvertFromDecimal() {
    final String result = udf.chr(75);
    assertThat(result, is("K"));
  }

  @Test
  public void shouldConvertFromUTF16String() {
    final String result = udf.chr("\\u004b");
    assertThat(result, is("K"));
  }

  @Test
  public void shouldConvertFromUTF16StringWithSlash() {
    final String result = udf.chr("\\u004b");
    assertThat(result, is("K"));
  }

  @Test
  public void shouldConvertZhFromDecimal() {
    final String result = udf.chr(22909);
    assertThat(result, is("好"));
  }

  @Test
  public void shouldConvertZhFromUTF16() {
    final String result = udf.chr("\\u597d");
    assertThat(result, is("好"));
  }

  @Test
  public void shouldConvertControlChar() {
    final String result = udf.chr(9);
    assertThat(result, is("\t"));
  }

  @Test
  public void shouldReturnNullForNullIntegerInput() {
    final String result = udf.chr((Integer) null);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullStringInput() {
    final String result = udf.chr((String) null);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForEmptyStringInput() {
    final String result = udf.chr("");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNegativeDecimalCode() {
    final String result = udf.chr(-1);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnSingleCharForMaxBMPDecimal() {
    final String result = udf.chr(65535);
    assertThat(result.codePointAt(0), is(65535));
    assertThat(result.toCharArray().length, is(1));
  }

  @Test
  public void shouldReturnTwoCharsForNonBMPDecimal() {
    final String result = udf.chr(65536);
    assertThat(result.codePointAt(0), is(65536));
    assertThat(result.toCharArray().length, is(2));
  }

  @Test
  public void shouldReturnTwoCharsForMaxUnicodeDecimal() {
    final String result = udf.chr(1_114_111);
    assertThat(result.codePointAt(0), is(1_114_111));
    assertThat(result.toCharArray().length, is(2));
  }

  @Test
  public void shouldReturnNullForOutOfRangeDecimal() {
    final String result = udf.chr(1_114_112);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForTooShortUTF16String() {
    final String result = udf.chr("\\u065");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnTwoCharsForNonBMPString() {
    final String result = udf.chr("\\ud800\\udc01");
    assertThat(result.codePointAt(0), is(65537));
    assertThat(result.toCharArray().length, is(2));
  }

}
