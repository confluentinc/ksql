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

package io.confluent.ksql.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.schema.ksql.FormatOptions;
import org.junit.Test;

public class IdentifiersTest {

  @Test
  public void shouldEscapeReservedWordWithBackTicks() {
    // Given:
    final FormatOptions options = FormatOptions.of(word -> true);

    // When:
    final String escaped = Identifiers.escape("foo", options);

    // Then:
    assertThat(escaped, is("`foo`"));
  }

  @Test
  public void shouldNotEscapeUnReservedWord() {
    // Given:
    final FormatOptions options = FormatOptions.of(word -> false);

    // When:
    final String escaped = Identifiers.escape("foo", options);

    // Then:
    assertThat(escaped, is("foo"));
  }

  @Test
  public void shouldEnsureTrimmed() {
    // When
    assertThrows(
        IllegalArgumentException.class,
        () -> Identifiers.ensureTrimmed(" foo ", "foo")
    );
  }

  @Test
  public void shouldEnsureTrimmedWhenEmpty() {
    // When
    assertThrows(
        IllegalArgumentException.class,
        () -> Identifiers.ensureTrimmed("", "foo")
    );
  }

}