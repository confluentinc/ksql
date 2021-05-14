/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.function.udf.string;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNull;

import io.confluent.ksql.function.KsqlFunctionException;
import org.junit.Test;

public class RegexpExtractAllTest {
  private RegexpExtractAll udf = new RegexpExtractAll();

  @Test
  public void shouldReturnNullOnNullValue() {
    assertNull(udf.regexpExtractAll(null, null));
    assertNull(udf.regexpExtractAll(null, null, null));
    assertNull(udf.regexpExtractAll(null, "", 1));
    assertNull(udf.regexpExtractAll("some string", null, 1));
    assertNull(udf.regexpExtractAll("some string", "", null));
  }

  @Test
  public void shouldReturnSubstringWhenMatched() {
    assertThat(udf.regexpExtractAll("e.*", "test string"), contains("est string"));
    assertThat(udf.regexpExtractAll(".", "test"), contains("t", "e", "s", "t"));
    assertThat(udf.regexpExtractAll("[AEIOU].{4}", "usEr nAmE 1308"), contains("Er nA", "E 130"));
  }

  @Test
  public void shouldReturnEmptyWhenNoMatch() {
    assertThat(udf.regexpExtractAll("tst", "test string"), empty());
  }

  @Test
  public void shouldReturnSubstringCapturedByGroupNumber() {
    assertThat(udf.regexpExtractAll("(\\w*) (\\w*)", "test string", 1), contains("test"));
    assertThat(udf.regexpExtractAll("(\\w*) (\\w*)", "test string", 2), contains("string"));

    assertThat(udf.regexpExtractAll("(\\w*) (\\w*)", "test string another one", 1),
        contains("test", "", ""));
    assertThat(udf.regexpExtractAll("(\\w*) (\\w*)", "test string another one", 2),
        contains("string", "another", "one"));
    assertThat(udf.regexpExtractAll("(\\w+) (\\w+)", "test string another one", 1),
        contains("test", "another"));
    assertThat(udf.regexpExtractAll("(\\w+) (\\w+)", "test string another one", 2),
        contains("string", "one"));

    assertThat(udf.regexpExtractAll("(a)(b)?", "ab a", 1), contains("a", "a"));
    assertThat(udf.regexpExtractAll("(a)(b)?", "ab a", 2), contains("b", null));
  }

  @Test
  public void shouldReturnNullIfGivenGroupNumberGreaterThanAvailableGroupNumbers() {
    assertThat(udf.regexpExtractAll("e", "test string", 3), nullValue());
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldHaveBadPattern() {
    udf.regexpExtractAll("(()", "test string");
  }
}
