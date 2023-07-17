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

package io.confluent.ksql.rest.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.rest.util.ConcurrencyLimiter.Decrementer;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConcurrencyLimiterTest {

  @Test
  public void shouldSucceedUnderLimit() {
    // Given:
    ConcurrencyLimiter limiter = new ConcurrencyLimiter(1, "pull queries");

    // When:
    Decrementer decrementer = limiter.increment();

    // Then:
    assertThat(limiter.getCount(), is(1));
    decrementer.decrementAtMostOnce();
    decrementer.decrementAtMostOnce();
    assertThat(limiter.getCount(), is(0));
  }

  @Test
  public void shouldError_atLimit() {
    // Given:
    ConcurrencyLimiter limiter = new ConcurrencyLimiter(1, "pull queries");

    // When:
    Decrementer decrementer = limiter.increment();
    final Exception e = assertThrows(
        KsqlException.class,
        limiter::increment
    );

    // Then:
    assertThat(e.getMessage(), containsString("Host is at concurrency limit for pull queries."));
    assertThat(limiter.getCount(), is(1));
    decrementer.decrementAtMostOnce();
    assertThat(limiter.getCount(), is(0));
  }
}
