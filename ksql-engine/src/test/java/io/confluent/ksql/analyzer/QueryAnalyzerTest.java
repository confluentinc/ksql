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

package io.confluent.ksql.analyzer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Sink;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryAnalyzerTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private MetaStore metaStore;
  @Mock
  private Analyzer analyzer;
  @Mock
  private Query query;
  @Mock
  private Analysis analysis;
  @Mock
  private QueryValidator continuousValidator;
  @Mock
  private QueryValidator staticValidator;
  @Mock
  private Sink sink;
  private QueryAnalyzer queryAnalyzer;

  @Before
  public void setUp() {
    queryAnalyzer = new QueryAnalyzer(
        metaStore,
        analyzer,
        continuousValidator,
        staticValidator
    );

    when(analyzer.analyze(any(), any())).thenReturn(analysis);
  }

  @Test
  public void shouldPreThenPostValidateContinuousQueries() {
    // Given:
    when(query.isStatic()).thenReturn(false);

    // When:
    queryAnalyzer.analyze(query, Optional.of(sink));

    // Then:
    final InOrder inOrder = Mockito.inOrder(continuousValidator);
    inOrder.verify(continuousValidator).preValidate(query, Optional.of(sink));
    inOrder.verify(continuousValidator).postValidate(analysis);
    verifyNoMoreInteractions(staticValidator);
  }

  @Test
  public void shouldPreValidateStaticQueries() {
    // Given:
    when(query.isStatic()).thenReturn(true);

    // When:
    queryAnalyzer.analyze(query, Optional.of(sink));

    // Then:
    final InOrder inOrder = Mockito.inOrder(staticValidator);
    inOrder.verify(staticValidator).preValidate(query, Optional.of(sink));
    inOrder.verify(staticValidator).postValidate(analysis);
    verifyNoMoreInteractions(continuousValidator);
  }
}