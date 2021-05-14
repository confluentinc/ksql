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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;

import io.confluent.ksql.function.KsqlFunctionException;
import org.junit.Test;

public class RegexpReplaceTest {

  private RegexpReplace udf = new RegexpReplace();

  @Test
  public void shouldHandleNull() {
    assertThat(udf.regexpReplace(null, "foo", "bar"), isEmptyOrNullString());
    assertThat(udf.regexpReplace("foo", null, "bar"), isEmptyOrNullString());
    assertThat(udf.regexpReplace("foo", "bar", null), isEmptyOrNullString());
  }

  @Test
  public void shouldReplace() {
    assertThat(udf.regexpReplace("foobar", "foo", "bar"), is("barbar"));
    assertThat(udf.regexpReplace("foobar", "fooo", "bar"), is("foobar"));
    assertThat(udf.regexpReplace("foobar", "o", ""), is("fbar"));
    assertThat(udf.regexpReplace("abc", "", "n"), is("nanbncn"));

    assertThat(udf.regexpReplace("foobar", "(foo|bar)", "cat"), is("catcat"));
    assertThat(udf.regexpReplace("foobar", "^foo", "cat"), is("catbar"));
    assertThat(udf.regexpReplace("foobar", "^bar", "cat"), is("foobar"));
    assertThat(udf.regexpReplace("barbar", "bar$", "cat"), is("barcat"));
    assertThat(udf.regexpReplace("aababa", "ab", "xy"), is("axyxya"));
    assertThat(udf.regexpReplace("aababa", "(ab)+", "xy"), is("axya"));
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowExceptionOnBadPattern() {
    udf.regexpReplace("foobar", "(()", "bar");
  }
}
