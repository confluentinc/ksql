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

package io.confluent.ksql.query.id;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Before;
import org.junit.Test;

public class SequentialQueryIdGeneratorTest {

  private SequentialQueryIdGenerator generator;

  @Before
  public void setUp() {
    generator = new SequentialQueryIdGenerator();
  }

  @Test
  public void shouldGenerateMonotonicallyIncrementingIds() {
    assertThat(generator.getNext(), is(0L));
    assertThat(generator.getNext(), is(1L));
    assertThat(generator.getNext(), is(2L));
  }

  @Test
  public void shouldNotIncrementWhenPeekNext() {
    assertThat(generator.getNext(), is(0L));
    assertThat(generator.peekNext(), is(1L));
    assertThat(generator.getNext(), is(1L));
  }

  @Test
  public void shouldCopy() {
    // When:
    final QueryIdGenerator copy = generator.createSandbox();

    // Then:
    assertThat(copy.getNext(), is(generator.getNext()));
    assertThat(copy.getNext(), is(generator.getNext()));
  }
}