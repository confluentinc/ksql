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

package io.confluent.ksql.function.udf.json;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.KudfTester;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

public class JsonExtractStringKudfTest {
  private final String JSON_DOC = "{"
                                  + "\"thing1\":{\"thing2\":\"hello\"},"
                                  + "\"array\":[101,102]"
                                  + "}";

  private JsonExtractStringKudf udf;

  @Before
  public void setUp() {
    udf = new JsonExtractStringKudf();
  }

  @Test
  public void shouldBeWellBehavedUdf() {
    new KudfTester(JsonExtractStringKudf::new)
        .withArguments(JSON_DOC, "$.thing1")
        .withNonNullableArgument(1)
        .test();
  }

  @Test
  public void shouldExtractJsonField() {
    // When:
    final Object result = udf.evaluate(JSON_DOC, "$.thing1.thing2");

    // Then:
    assertThat(result, is("hello"));
  }

  @Test
  public void shouldExtractJsonDoc() {
    // When:
    final Object result = udf.evaluate(JSON_DOC, "$.thing1");

    // Then:
    assertThat(result, is("{\"thing2\":\"hello\"}"));
  }

  @Test
  public void shouldExtractWholeJsonDoc() {
    // When:
    final Object result = udf.evaluate(JSON_DOC, "$");

    // Then:
    assertThat(result, is(JSON_DOC));
  }

  @Test
  public void shouldExtractJsonArrayField() {
    // When:
    final Object result = udf.evaluate(JSON_DOC, "$.array.1");

    // Then:
    assertThat(result, is("102"));
  }

  @Test
  public void shouldReturnNullIfNodeNotFound() {
    // When:
    final Object result = udf.evaluate(JSON_DOC, "$.will.not.find.me");

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowIfTooFewParameters() {
    udf.evaluate(JSON_DOC);
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowIfTooManyParameters() {
    udf.evaluate(JSON_DOC, "$.thing1", "extra");
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowOnInvalidJsonDoc() {
    udf.evaluate("this is NOT a JSON doc", "$.thing1");
  }

  @Test
  public void shouldBeThreadSafe() {
    IntStream.range(0, 10_000)
        .parallel()
        .forEach(idx -> shouldExtractJsonField());
  }
}