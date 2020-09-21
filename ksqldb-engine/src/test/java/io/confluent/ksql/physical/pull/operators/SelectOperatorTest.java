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

package io.confluent.ksql.physical.pull.operators;

import static org.mockito.Mockito.verify;

import io.confluent.ksql.planner.plan.FilterNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SelectOperatorTest {

  @Mock
  private FilterNode logicalNode;
  @Mock
  private AbstractPhysicalOperator child;

  @Test
  public void shouldOpenChild() {
    // Given:
    SelectOperator selectOperator = new SelectOperator(logicalNode);
    selectOperator.addChild(child);
    selectOperator.open();

    // Then:
    verify(child).open();
  }

  @Test
  public void shouldReturnNextOfChild() {
    // Given:
    SelectOperator selectOperator = new SelectOperator(logicalNode);
    selectOperator.addChild(child);
    selectOperator.next();

    // Then:
    verify(child).next();
  }

}
