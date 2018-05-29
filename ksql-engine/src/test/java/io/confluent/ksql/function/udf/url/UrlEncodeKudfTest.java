/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.ksql.function.udf.url;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import io.confluent.ksql.function.KsqlFunctionException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Before;

public class UrlEncodeKudfTest {

  private static final String inputValueWithEncodableChars = "?foo $bar";
  private static final String inputWithSpecialChars = "foo.-*_bar";

  private UrlEncodeKudf encodeUdf;

  @Before
  public void setUp() {
    encodeUdf = new UrlEncodeKudf();
  }

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldEncodeValue() {
    assertEquals("%3Ffoo+%24bar", encodeUdf.evaluate(inputValueWithEncodableChars));
  }

  @Test
  public void shouldReturnSpecialCharsIntact() {
    //the characters of ". - * _" should all pass through as-is
    assertEquals(inputWithSpecialChars, encodeUdf.evaluate(inputWithSpecialChars));
  }

  @Test
  public void shouldReturnEmptyStringForEmptyInput() {
    assertEquals("", encodeUdf.evaluate(""));
  }

  @Test
  public void shouldFailWithTooFewParams() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("url_encode udf requires one input");
    encodeUdf.evaluate();
  }

  @Test
  public void shouldFailWithTooManyParams() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("url_encode udf requires one input");
    encodeUdf.evaluate("foo", "bar");
  }
}
