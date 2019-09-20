/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Before;
import org.junit.Test;

public class HybridQueryIdGeneratorTest {

  private HybridQueryIdGenerator generator;

  @Before
  public void setUp() {
    generator = new HybridQueryIdGenerator();
  }

  @Test
  public void shouldSwitchBetweenGeneratorsAfterUpdate() {
    assertThat(generator.getNextId(), is("0"));
    assertThat(generator.getNextId(), is("1"));
    assertThat(generator.getNextId(), is("2"));
    generator.updateOffset(5L);
    assertThat(generator.getNextId(), is("5"));
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowWhenGetNextIdBeforeUpdate() {
    assertThat(generator.getNextId(), is("0"));
    assertThat(generator.getNextId(), is("1"));
    generator.updateOffset(5);
    generator.getNextId();
    generator.getNextId();
  }

  @Test
  public void shouldCopyDefaultQueryIdGeneratorCounterNoUpdate() {
    // When:
    generator.getNextId();
    generator.getNextId();
    final QueryIdGenerator copy = generator.createSandbox();

    // Then:
    assertThat(copy.getNextId(), is("2"));
  }

  @Test
  public void shouldCopyQueryIdGeneratorUsingOffsetValueWhenUpdate() {
    // When:
    generator.getNextId();
    generator.getNextId();
    generator.updateOffset(5);
    final QueryIdGenerator copy = generator.createSandbox();

    // Then:
    assertThat(copy.getNextId(), is("6"));
  }
}