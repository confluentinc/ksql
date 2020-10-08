/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.ksql.function.udf.string;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Before;
import org.junit.Test;

public class TrimTest {

  private Trim udf;

  @Before
  public void setUp() {
    udf = new Trim();
  }

  @Test
  public void shouldRemoveLeadingWhitespace() {
    final String result = udf.trim(" \t Foo Bar");
    assertThat(result, is("Foo Bar"));
  }

  @Test
  public void shouldRemoveTrailingWhitespace() {
    final String result = udf.trim("Foo Bar \t ");
    assertThat(result, is("Foo Bar"));
  }

  @Test
  public void shouldReturnNullForNullInput() {
    final String result = udf.trim(null);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnEmptyForEmptyInput() {
    final String result = udf.trim("");
    assertThat(result, is(""));
  }

  @Test
  public void shouldReturnEmptyForWhitespaceInput() {
    final String result = udf.trim(" \t ");
    assertThat(result, is(""));
  }
}
