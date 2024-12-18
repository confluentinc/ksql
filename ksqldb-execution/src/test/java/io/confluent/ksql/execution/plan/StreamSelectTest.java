/*
 * Copyright 2022 Confluent Inc.
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
import io.confluent.ksql.name.ColumnName;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("UnstableApiUsage")
@RunWith(MockitoJUnitRunner.class)
public class StreamSelectTest {

  @Mock
  private ExecutionStepPropertiesV1 properties1;
  @Mock
  private ExecutionStepPropertiesV1 properties2;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> source1;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> source2;

  private List<ColumnName> keys1;
  private List<ColumnName> keys2;
  private Optional<List<ColumnName>> selectedKeys1;
  private Optional<List<ColumnName>> selectedKeys2;
  private List<SelectExpression> selects1;
  private List<SelectExpression> selects2;

  @Before
  public void setup() {
    keys1 = ImmutableList.of(mock(ColumnName.class));
    keys2 = ImmutableList.of(mock(ColumnName.class));
    selectedKeys1 = Optional.of(ImmutableList.of(mock(ColumnName.class)));
    selectedKeys2 = Optional.of(ImmutableList.of(mock(ColumnName.class)));
    selects1 = ImmutableList.of(mock(SelectExpression.class));
    selects2 = ImmutableList.of(mock(SelectExpression.class));
  }

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new StreamSelect<>(properties1, source1, keys1, selectedKeys1, selects1),
            new StreamSelect<>(properties1, source1, keys1, selectedKeys1, selects1)
        )
        .addEqualityGroup(new StreamSelect<>(properties2, source1, keys1, selectedKeys1, selects1))
        .addEqualityGroup(new StreamSelect<>(properties1, source2, keys1, selectedKeys1, selects1))
        .addEqualityGroup(new StreamSelect<>(properties1, source1, keys2, selectedKeys1, selects1))
        .addEqualityGroup(new StreamSelect<>(properties1, source1, keys1, selectedKeys2, selects1))
        .addEqualityGroup(new StreamSelect<>(properties1, source1, keys1, Optional.empty(), selects1))
        .addEqualityGroup(new StreamSelect<>(properties1, source1, keys1, selectedKeys1, selects2))
        .testEquals();
  }
}