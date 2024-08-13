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

package io.confluent.ksql.function.udf.json;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.function.KsqlFunctionException;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

public class JsonExtractStringKudfTest {
  private static final String JSON_DOC = "{"
                                  + "\"thing1\":{\"thing2\":\"hello\"},"
                                  + "\"array\":[101,102]"
                                  + "}";

  private JsonExtractString udf;

  @Before
  public void setUp() {
    udf = new JsonExtractString();
  }

  @Test
  public void shouldExtractJsonField() {
    // When:
    final String result = udf.extract(JSON_DOC, "$.thing1.thing2");

    // Then:
    assertThat(result, is("hello"));
  }

  @Test
  public void shouldExtractJsonFieldWithAtCharacter() {
    // When:
    final String result = udf.extract("{\"@type\": \"foo\"}", "$.@type");

    // Then:
    assertThat(result, is("foo"));
  }

  @Test
  public void shouldExtractJsonDoc() {
    // When:
    final String result = udf.extract(JSON_DOC, "$.thing1");

    // Then:
    assertThat(result, is("{\"thing2\":\"hello\"}"));
  }

  @Test
  public void shouldExtractWholeJsonDoc() {
    // When:
    final String result = udf.extract(JSON_DOC, "$");

    // Then:
    assertThat(result, is(JSON_DOC));
  }

  @Test
  public void shouldExtractJsonArrayField() {
    // When:
    final String result = udf.extract(JSON_DOC, "$.array.1");

    // Then:
    assertThat(result, is("102"));
  }

  @Test
  public void shouldReturnNullIfNodeNotFound() {
    // When:
    final String result = udf.extract(JSON_DOC, "$.will.not.find.me");

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullDocString() {
    final String result = udf.extract(null, "$.thing1");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullPath() {
    final String result = udf.extract(JSON_DOC, null);
    assertThat(result, is(nullValue()));
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowOnInvalidJsonDoc() {
    udf.extract("this is NOT a JSON doc", "$.thing1");
  }

  @Test
  public void shouldBeThreadSafe() {
    IntStream.range(0, 10_000)
        .parallel()
        .forEach(idx -> shouldExtractJsonField());
  }

  @Test
  public void shouldParseCorrectlyDifferentPathsOnSameInstance() {
    final String thing1 = udf.extract(JSON_DOC, "$.thing1");
    assertThat(thing1, is("{\"thing2\":\"hello\"}"));

    final String array = udf.extract(JSON_DOC, "$.array.1");
    assertThat(array, is("102"));
  }
}
