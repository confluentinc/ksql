/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.confluent.ksql.query.QueryError.Type;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryErrorClassifierTest {

  @Mock
  private Throwable error;
  @Mock
  private QueryErrorClassifier classifierA;
  @Mock
  private QueryErrorClassifier classifierB;

  @Before
  public void setUp() {
    when(classifierA.and(any())).thenCallRealMethod();
  }

  @Test
  public void shouldChainUnknownUnknown() {
    // Given:
    when(classifierA.classify(any())).thenReturn(Type.UNKNOWN);
    when(classifierB.classify(any())).thenReturn(Type.UNKNOWN);

    // When:
    final Type type = classifierA.and(classifierB).classify(error);

    // Then:
    assertThat(type, is(Type.UNKNOWN));
  }

  @Test
  public void shouldChainUnknownSomething() {
    // Given:
    when(classifierA.classify(any())).thenReturn(Type.UNKNOWN);
    when(classifierB.classify(any())).thenReturn(Type.USER);

    // When:
    final Type type = classifierA.and(classifierB).classify(error);

    // Then:
    assertThat(type, is(Type.USER));
  }

  @Test
  public void shouldChainSomethingUnknown() {
    // Given:
    when(classifierA.classify(any())).thenReturn(Type.USER);
    when(classifierB.classify(any())).thenReturn(Type.UNKNOWN);

    // When:
    final Type type = classifierA.and(classifierB).classify(error);

    // Then:
    assertThat(type, is(Type.USER));
  }

  @Test
  public void shouldChainIdentical() {
    // Given:
    when(classifierA.classify(any())).thenReturn(Type.USER);
    when(classifierB.classify(any())).thenReturn(Type.USER);

    // When:
    final Type type = classifierA.and(classifierB).classify(error);

    // Then:
    assertThat(type, is(Type.USER));
  }

  @Test
  public void shouldChainDiffering() {
    // Given:
    when(classifierA.classify(any())).thenReturn(Type.USER);
    when(classifierB.classify(any())).thenReturn(Type.SYSTEM);

    // When:
    final Type type = classifierA.and(classifierB).classify(error);

    // Then:
    assertThat(type, is(Type.UNKNOWN));
  }

}