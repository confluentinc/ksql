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

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;

public class ReplaceTest {
  private Replace udf;

  @Before
  public void setUp() {
    udf = new Replace();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.replace(null, "foo", "bar"), isEmptyOrNullString());
    assertThat(udf.replace("foo", null, "bar"), isEmptyOrNullString());
    assertThat(udf.replace("foo", "bar", null), isEmptyOrNullString());
  }

  @Test
  public void shouldReplace() {
    assertThat(udf.replace("foobar", "foo", "bar"), is("barbar"));
    assertThat(udf.replace("foobar", "fooo", "bar"), is("foobar"));
    assertThat(udf.replace("foobar", "o", ""), is("fbar"));
    assertThat(udf.replace("abc", "", "n"), is("nanbncn"));
  }
}