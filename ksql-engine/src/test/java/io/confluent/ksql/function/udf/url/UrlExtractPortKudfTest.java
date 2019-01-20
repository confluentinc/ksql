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

import io.confluent.ksql.util.KsqlException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class UrlExtractPortKudfTest {

  private UrlExtractPortKudf extractUdf;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() {
    extractUdf = new UrlExtractPortKudf();
  }

  @Test
  public void shouldExtractPortIfPresent() {
    assertThat(
            extractUdf.extractPort("https://docs.confluent.io:8080/current/ksql/docs/syntax-reference.html#scalar-functions"),
            equalTo(8080));
  }

  @Test
  public void shouldReturnNullIfNoPort() {
    assertThat(
            extractUdf.extractPort("https://docs.confluent.io/current/ksql/docs/syntax-reference.html#scalar-functions"),
            equalTo(null));
  }

  @Test
  public void shouldThrowExceptionForMalformedURL() {
    // Given:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("The passed in URL http://257.1/bogus/[url has invalid syntax!");

    // When:
    extractUdf.extractPort("http://257.1/bogus/[url");
  }

}
