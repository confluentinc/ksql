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

public class UrlExtractPortKudfTest {

  private static final String inputUrl =
      "https://docs.confluent.io/current/ksql/docs/syntax-reference.html#scalar-functions";
  private static final String bogusUrl = "http://257.1/bogus/[url";
  private static final String inputUrlWithPort =
      "https://docs.confluent.io:8080/current/ksql/docs/syntax-reference.html#scalar-functions";

  private UrlExtractPortKudf extractUdf;

  @Before
  public void setUp(){
    extractUdf = new UrlExtractPortKudf();
  }
  
  @Rule
  public final ExpectedException expectedException = ExpectedException.none();


  @Test
  public void shouldExtractPortIfPresent() {
    assertEquals(8080, extractUdf.evaluate(inputUrlWithPort));
  }

  @Test
  public void shouldReturnNullIfNoPort() {
    assertNull(extractUdf.evaluate(inputUrl));
  }

  @Test
  public void shouldReturnNullForInvalidUrl() {
    assertNull(extractUdf.evaluate(bogusUrl));
  }

  @Test
  public void shouldFailWithTooFewParams() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("url_extract_port udf requires one input");
    extractUdf.evaluate();
  }

  @Test
  public void shouldFailWithTooManyParams() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("url_extract_port udf requires one input");
    extractUdf.evaluate("foo", "bar");
  }
}
