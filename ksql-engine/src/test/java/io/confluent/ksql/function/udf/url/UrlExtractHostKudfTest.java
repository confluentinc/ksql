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

import io.confluent.ksql.util.KsqlException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
