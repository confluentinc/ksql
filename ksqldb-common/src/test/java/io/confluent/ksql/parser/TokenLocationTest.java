/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.parser;

import java.util.OptionalInt;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class TokenLocationTest {

  @Test
  public void shouldBeEqualEmptyToken() {
    // When:
    final TokenLocation first = TokenLocation.empty();
    final TokenLocation second = TokenLocation.empty();

    // Then:
    assertThat(first, is(second));
  }

  @Test
  public void shouldBeEqualFirstTwoArgs() {
    // When:
    final TokenLocation first = TokenLocation.of(1, 2);
    final TokenLocation second = TokenLocation.of(1, 2);

    // Then:
    assertThat(first, is(second));
  }

  @Test
  public void shouldBeEqualTokens() {
    // When:
    final TokenLocation first = new TokenLocation(
        OptionalInt.of(1),
        OptionalInt.of(2),
        OptionalInt.of(3),
        OptionalInt.of(4)
    );
    final TokenLocation second = new TokenLocation(
        OptionalInt.of(1),
        OptionalInt.of(2),
        OptionalInt.of(3),
        OptionalInt.of(4)
    );

    // Then:
    assertThat(first, is(second));
  }

}