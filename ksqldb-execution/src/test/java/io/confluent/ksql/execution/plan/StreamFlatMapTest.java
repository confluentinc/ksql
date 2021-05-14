/*
 * Copyright 2021 Confluent Inc.
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
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("UnstableApiUsage")
@RunWith(MockitoJUnitRunner.class)
public class StreamFlatMapTest {
  @Mock
  private ExecutionStepPropertiesV1 properties1;
  @Mock
  private ExecutionStepPropertiesV1 properties2;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> source1;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> source2;

  private List<FunctionCall> functions1;
  private List<FunctionCall> functions2;

  @Before
  public void setup() {
    functions1 = ImmutableList.of(mock(FunctionCall.class), mock(FunctionCall.class));
    functions2 = ImmutableList.of(mock(FunctionCall.class), mock(FunctionCall.class));
  }

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new StreamFlatMap<>(properties1, source1, functions1),
            new StreamFlatMap<>(properties1, source1, functions1))
        .addEqualityGroup(new StreamFlatMap<>(properties2, source1, functions1))
        .addEqualityGroup(new StreamFlatMap<>(properties1, source2, functions1))
        .addEqualityGroup(new StreamFlatMap<>(properties1, source1, functions2))
        .testEquals();
  }
}