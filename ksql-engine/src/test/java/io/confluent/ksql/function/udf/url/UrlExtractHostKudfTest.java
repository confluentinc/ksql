/*
 * Copyright 2019 Confluent Inc.
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

import io.confluent.ksql.util.KsqlException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class UrlExtractHostKudfTest {

  private UrlExtractHostKudf extractUdf;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() {
    extractUdf = new UrlExtractHostKudf();
  }

  @Test
  public void shouldExtractHostIfPresent() {
    assertThat(extractUdf.extractHost("https://docs.confluent.io:8080/current/ksql/docs/syntax-reference.html#scalar-functions"), equalTo("docs.confluent.io"));
  }

  @Test
  public void shouldReturnNullIfNoHost() {
    assertThat(extractUdf.extractHost("https:///current/ksql/docs/syntax-reference.html#scalar-functions"), nullValue());
  }

  @Test
  public void shouldThrowExceptionForMalformedURL() {
    // Given:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("URL input has invalid syntax: http://257.1/bogus/[url");

    // When:
    extractUdf.extractHost("http://257.1/bogus/[url");
  }

}
