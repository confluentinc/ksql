/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.plan;

import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.name.ColumnName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StreamAggregateTest {
  @Mock
  private ExecutionStepPropertiesV1 properties1;
  @Mock
  private ExecutionStepPropertiesV1 properties2;
  @Mock
  private ExecutionStep<KGroupedStreamHolder> source1;
  @Mock
  private ExecutionStep<KGroupedStreamHolder> source2;
  @Mock
  private Formats formats1;
  @Mock
  private Formats formats2;

  private ImmutableList<ColumnName> columnRefs1;
  private ImmutableList<ColumnName> columnRefs2;
  private ImmutableList<FunctionCall> functionCalls1;
  private ImmutableList<FunctionCall> functionCalls2;

  @Before
  public void setup() {
    columnRefs1 = ImmutableList.of(mock(ColumnName.class), mock(ColumnName.class));
    columnRefs2 = ImmutableList.of(mock(ColumnName.class), mock(ColumnName.class));
    functionCalls1 = ImmutableList.of(mock(FunctionCall.class), mock(FunctionCall.class));
    functionCalls2 = ImmutableList.of(mock(FunctionCall.class), mock(FunctionCall.class));
  }

  @Test
  public void shouldImplementEqualsCorrectly() {
    new EqualsTester()
        .addEqualityGroup(
            new StreamAggregate(properties1, source1, formats1, columnRefs1, functionCalls1),
            new StreamAggregate(properties1, source1, formats1, columnRefs1, functionCalls1)
        ).addEqualityGroup(
            new StreamAggregate(properties2, source1, formats1, columnRefs1, functionCalls1)
        ).addEqualityGroup(
            new StreamAggregate(properties1, source2, formats1, columnRefs1, functionCalls1)
        ).addEqualityGroup(
            new StreamAggregate(properties1, source1, formats2, columnRefs1, functionCalls1)
        ).addEqualityGroup(
            new StreamAggregate(properties1, source1, formats1, columnRefs2, functionCalls1)
        ).addEqualityGroup(
            new StreamAggregate(properties1, source1, formats1, columnRefs1, functionCalls2)
        );
  }
}