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

package io.confluent.ksql.query.id;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.util.KsqlServerException;
import org.junit.Before;
import org.junit.Test;

public class SpecificQueryIdGeneratorTest {

  private SpecificQueryIdGenerator generator;

  @Before
  public void setUp() {
    generator = new SpecificQueryIdGenerator();
  }

  @Test
  public void shouldGenerateIdBasedOnSetNextId() {
    generator.setNextId(3L);
    assertThat(generator.getNext(), is("3"));
    generator.setNextId(5L);
    assertThat(generator.getNext(), is("5"));
  }

  @Test
  public void shouldReturnZeroIdForFirstQuery() {
    assertThat(generator.getNext(), is("0"));
  }

  @Test(expected = KsqlServerException.class)
  public void shouldThrowWhenGetNextBeforeSet() {
    generator.setNextId(3L);
    generator.getNext();
    generator.getNext();
  }

  @Test
  public void shouldReturnSequentialGeneratorFromLastId() {
    generator.setNextId(3L);
    final QueryIdGenerator copy = generator.createSandbox();
    assertThat(copy, instanceOf(SequentialQueryIdGenerator.class));
    assertThat(copy.getNext(), is("3"));
  }
}