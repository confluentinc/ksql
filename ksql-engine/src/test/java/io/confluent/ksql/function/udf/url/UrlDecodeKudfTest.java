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

public class UrlDecodeKudfTest {

  private static final String inputEncodedValue = "%3Ffoo+%24bar";
  private static final String inputSpecialChars = "foo.-*_bar";

  private UrlDecodeKudf decodeUdf;

  @Before
  public void setUp() {
    decodeUdf = new UrlDecodeKudf();
  }

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldDecodeEncodedValue() {
    assertEquals("?foo $bar", decodeUdf.evaluate(inputEncodedValue));
  }

  @Test
  public void shouldReturnSpecialCharsIntact() {
    //the characters of ". - * _" should all pass through as-is
    assertEquals(inputSpecialChars, decodeUdf.evaluate(inputSpecialChars));
  }

  @Test
  public void shouldReturnEmptyStringForEmptyInput() {
    assertEquals("", decodeUdf.evaluate(""));
  }

  @Test
  public void shouldFailWithTooFewParams() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("url_decode udf requires one input");
    decodeUdf.evaluate();
  }

  @Test
  public void shouldFailWithTooManyParams() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("url_decode udf requires one input");
    decodeUdf.evaluate("foo", "bar");
  }
}
