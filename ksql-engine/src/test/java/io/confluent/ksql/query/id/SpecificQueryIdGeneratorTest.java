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

package io.confluent.ksql.query.id;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Before;
import org.junit.Test;

import java.util.NoSuchElementException;

public class SpecificQueryIdGeneratorTest {

  private SpecificQueryIdGenerator generator;

  @Before
  public void setUp() {
    generator = new SpecificQueryIdGenerator();
  }

  @Test
  public void shouldGenerateIdBasedOnSetNextId() {
    generator.setNextId(3L);
    assertThat(generator.getNext(), is(3L));
    generator.setNextId(5L);
    assertThat(generator.getNext(), is(5L));
  }

  @Test
  public void shouldReturnNextIdWhenPeekNext() {
    generator.setNextId(3L);
    assertThat(generator.peekNext(), is(3L));
  }

  @Test
  public void shouldReturnNextIdIncrementedWhenPeekNextAfterGetNext() {
    generator.setNextId(3L);
    generator.getNext();
    assertThat(generator.peekNext(), is(4L));
  }

  @Test(expected = NoSuchElementException.class)
  public void shouldThrowWhenGetNextBeforeSet() {
    generator.setNextId(3L);
    generator.getNext();
    generator.getNext();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void shouldCopy() {
    generator.createSandbox();
  }
}