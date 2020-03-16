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

package io.confluent.ksql.statement;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.parser.tree.Statement;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class InjectorChainTest {

  @Mock
  ConfiguredStatement<Statement> statement;
  @Mock
  ConfiguredStatement<Statement> statement2;
  @Mock
  ConfiguredStatement<Statement> statement3;

  @Mock
  Injector injector1;
  @Mock
  Injector injector2;

  @Before
  public void setUp() {
    when(injector1.inject(statement)).thenReturn(statement2);
    when(injector2.inject(statement2)).thenReturn(statement3);
  }

  @Test
  public void shouldMultipleApplyInjectorsInOrder() {
    // When:
    InjectorChain.of(injector1, injector2).inject(statement);

    // Then:
    final InOrder inOrder = inOrder(injector1, injector2);
    inOrder.verify(injector1).inject(statement);
    inOrder.verify(injector2).inject(statement2);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldNotAllowModifiedArrays() {
    // Given:
    final Injector[] injectors = new Injector[]{injector1};
    final InjectorChain injectorChain = InjectorChain.of(injectors);

    // When:
    injectors[0] = injector2;
    injectorChain.inject(statement);

    // Then:
    verify(injector1).inject(statement);
    verify(injector2, never()).inject(any());
  }

}