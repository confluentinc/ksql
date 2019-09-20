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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.junit.Before;
import org.junit.Test;


public class HybridQueryIdGeneratorTest {

  private final SequentialQueryIdGenerator sequentialQueryIdGenerator =
      mock(SequentialQueryIdGenerator.class);
  private final SpecificQueryIdGenerator specificQueryIdGenerator =
      mock(SpecificQueryIdGenerator.class);

  private HybridQueryIdGenerator generator;

  @Before
  public void setUp() {
    generator = new HybridQueryIdGenerator(
        sequentialQueryIdGenerator,
        specificQueryIdGenerator);
  }

  @Test
  public void shouldUseLegacyGeneratorByDefault() {
    // Given:
    when(sequentialQueryIdGenerator.getNext()).thenReturn(3L);
    when(specificQueryIdGenerator.getNext()).thenReturn(5L);

    // Then:
    assertThat(generator.getNext(), is(3L));
  }

  @Test
  public void shouldUseNewGenerator() {
    // Given:
    when(sequentialQueryIdGenerator.getNext()).thenReturn(3L);
    when(specificQueryIdGenerator.getNext()).thenReturn(5L);
    generator.activateNewGenerator(5L);

    // Then:
    assertThat(generator.getNext(), is(5L));
    verify(specificQueryIdGenerator, times(1)).setNextId(5L);
  }

  @Test
  public void shouldSwitchBetweenGenerators() {
    // Given:
    when(sequentialQueryIdGenerator.getNext()).thenReturn(3L);
    when(specificQueryIdGenerator.getNext()).thenReturn(5L);
    generator.activateNewGenerator(anyLong());
    generator.activateLegacyGenerator();

    // Then:
    assertThat(generator.getNext(), is(3L));
  }


  @Test
  public void shouldUseActiveGeneratorWhenPeekNext() {
    // Given:
    when(sequentialQueryIdGenerator.peekNext()).thenReturn(3L);
    when(specificQueryIdGenerator.peekNext()).thenReturn(5L);

    // Then:
    assertThat(generator.peekNext(), is(3L));
    generator.activateNewGenerator(5L);
    assertThat(generator.peekNext(), is(5L));
    generator.activateLegacyGenerator();
    assertThat(generator.peekNext(), is(3L));
  }


  @Test
  public void shouldSandboxActiveGenerator() {
    // Given:
    when(sequentialQueryIdGenerator.peekNext()).thenReturn(3L);
    when(specificQueryIdGenerator.peekNext()).thenReturn(5L);

    // Then:
    final QueryIdGenerator copyGenerator1 = generator.createSandbox();
    assertThat(copyGenerator1.peekNext(), is(3L));

    generator.activateNewGenerator(anyLong());
    final QueryIdGenerator copyGenerator12 = generator.createSandbox();
    assertThat(copyGenerator12.peekNext(), is(5L));
  }


}