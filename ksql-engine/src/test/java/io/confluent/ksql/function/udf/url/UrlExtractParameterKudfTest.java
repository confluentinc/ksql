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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import org.junit.Before;

public class UrlExtractParameterKudfTest {

  private UrlExtractParameterKudf extractUdf;

  @Before
  public void setUp() {
    extractUdf = new UrlExtractParameterKudf();
  }

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();


  @Test
  public void shouldExtractParamValueIfPresent() {
    assertThat(extractUdf.evaluate("https://docs.confluent.io/current/ksql/docs/syntax-reference.html?foo%20bar=baz&blank#scalar-functions", "foo bar"), equalTo("baz"));
  }

  @Test
  public void shouldReturnNullIfParamHasNoValue() {
    assertThat(extractUdf.evaluate("https://docs.confluent.io/current/ksql/docs/syntax-reference.html?foo%20bar=baz&blank#scalar-functions", "blank"), nullValue());
  }

  @Test
  public void shouldReturnNullIfParamNotPresent() {
    assertThat(extractUdf.evaluate("https://docs.confluent.io/current/ksql/docs/syntax-reference.html?foo%20bar=baz&blank#scalar-functions", "absent"), nullValue());
  }

  @Test
  public void shouldReturnNullForInvalidUrl() {
    assertThat(extractUdf.evaluate("http://257.1/bogus/[url", "foo bar"), nullValue());
  }

  @Test
  public void shouldFailWithTooFewParams() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("url_extract_parameter udf requires two input");
    extractUdf.evaluate();
  }

  @Test
  public void shouldFailWithTooManyParams() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("url_extract_parameter udf requires two input");
    extractUdf.evaluate("foo", "bar", "baz");
  }
}
