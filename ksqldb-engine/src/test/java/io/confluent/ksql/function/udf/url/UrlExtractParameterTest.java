/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
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
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.util.KsqlException;
import org.junit.Before;
import org.junit.Test;

public class UrlExtractParameterTest {

  private UrlExtractParameter extractUdf;

  @Before
  public void setUp() {
    extractUdf = new UrlExtractParameter();
  }

  @Test
  public void shouldExtractParamValueIfPresent() {
    assertThat(extractUdf.extractParam("https://docs.confluent.io?foo%20bar=baz%20zab&blank#scalar-functions", "foo bar"), equalTo("baz zab"));
  }

  @Test
  public void shouldExtractParamValueCaseSensitive() {
    assertThat(extractUdf.extractParam("https://docs.confluent.io?foo%20bar=baz&blank#scalar-functions", "foo Bar"), nullValue());
  }

  @Test
  public void shouldReturnEmptyStringIfParamHasNoValue() {
    assertThat(extractUdf.extractParam("https://docs.confluent.io?foo%20bar=baz&blank#scalar-functions", "blank"), equalTo(""));
  }

  @Test
  public void shouldReturnNullIfParamNotPresent() {
    assertThat(extractUdf.extractParam("https://docs.confluent.io?foo%20bar=baz&blank#scalar-functions", "absent"), nullValue());
  }

  @Test
  public void shouldThrowExceptionForMalformedURL() {
    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> extractUdf.extractParam("http://257.1/bogus/[url", "foo bar")
    );

    // Given:
    assertThat(e.getMessage(), containsString("URL input has invalid syntax: http://257.1/bogus/[url"));
  }
}
