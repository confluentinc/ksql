/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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
import static org.hamcrest.Matchers.equalTo;

import org.junit.Test;

public class FieldTest {

  private final Field field = new Field();

  @Test
  public void shouldFindFirstArgument() {
    // When:
    final int pos = field.field("hello", "hello", "world");

    // Then:
    assertThat(pos, equalTo(1));
  }

  @Test
  public void shouldFindSecondArgument() {
    // When:
    final int pos = field.field("world", "hello", "world");

    // Then:
    assertThat(pos, equalTo(2));
  }

  @Test
  public void shouldFindArgumentWhenOneIsNull() {
    // When:
    final int pos = field.field("world", null, "world");

    // Then:
    assertThat(pos, equalTo(2));
  }

  @Test
  public void shouldNotFindMissing() {
    // When:
    final int pos = field.field("missing", "hello", "world");

    // Then:
    assertThat(pos, equalTo(0));
  }

  @Test
  public void shouldNotFindIfNoArgs() {
    // When:
    final int pos = field.field("missing");

    // Then:
    assertThat(pos, equalTo(0));
  }

  @Test
  public void shouldNotFindNull() {
    // When:
    final int pos = field.field(null, null, "world");

    // Then:
    assertThat(pos, equalTo(0));
  }

  @Test
  public void shouldNotFindStringInNullArray() {
    // When:
    String[] array = null;
    final int pos = field.field("1", array);

    // Then:
    assertThat(pos, equalTo(0));
  }
}
