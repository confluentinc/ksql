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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class UrlExtractQueryKudfTest {

  private UrlExtractQueryKudf extractUdf;

  @Before
  public void setUp() {
    extractUdf = new UrlExtractQueryKudf();
  }

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();


  @Test
  public void shouldExtractQueryIfPresent() {
    assertThat(extractUdf.extractQuery("https://docs.confluent.io/current/ksql/docs/syntax-reference.html?a=b&foo%20bar=baz&c=d&e#scalar-functions"), equalTo("a=b&foo bar=baz&c=d&e"));
  }

  @Test
  public void shouldReturnNullIfNoQuery() {
    assertThat(extractUdf.extractQuery("https://current/ksql/docs/syntax-reference.html#scalar-functions"), nullValue());
  }

  @Test
  public void shouldReturnNullForInvalidUrl() {
    assertThat(extractUdf.extractQuery("http://257.1/bogus/[url"), nullValue());
  }

}
