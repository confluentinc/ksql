/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.function.udf.url;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
    assertThat(extractUdf.extractParam("https://docs.confluent.io?foo%20bar=baz&blank#scalar-functions", "foo bar"), equalTo("baz"));
  }

  @Test
  public void shouldExtractParamValueCaseSensitive() {
    assertThat(extractUdf.extractParam("https://docs.confluent.io?foo%20bar=baz&blank#scalar-functions", "foo Bar"), nullValue());
  }

  @Test
  public void shouldReturnNullIfParamHasNoValue() {
    assertThat(extractUdf.extractParam("https://docs.confluent.io?foo%20bar=baz&blank#scalar-functions", "blank"), nullValue());
  }

  @Test
  public void shouldReturnNullIfParamNotPresent() {
    assertThat(extractUdf.extractParam("https://docs.confluent.io?foo%20bar=baz&blank#scalar-functions", "absent"), nullValue());
  }

  @Test
  public void shouldReturnNullForInvalidUrl() {
    assertThat(extractUdf.extractParam("http://257.1/bogus/[url", "foo bar"), nullValue());
  }

}
