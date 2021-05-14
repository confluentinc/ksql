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

import static io.confluent.ksql.execution.plan.JoinType.INNER;
import static io.confluent.ksql.execution.plan.JoinType.LEFT;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.name.ColumnName;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TableTableJoinTest {

  private static final ColumnName KEY = ColumnName.of("Bob");
  private static final ColumnName KEY2 = ColumnName.of("Vic");

  @Mock
  private ExecutionStepPropertiesV1 props1;
  @Mock
  private ExecutionStepPropertiesV1 props2;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> left1;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> right1;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> left2;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> right2;

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new TableTableJoin<>(props1, INNER, KEY, left1, right1),
            new TableTableJoin<>(props1, INNER, KEY, left1, right1)
        )
        .addEqualityGroup(
            new TableTableJoin<>(props2, INNER, KEY, left1, right1)
        )
        .addEqualityGroup(
            new TableTableJoin<>(props1, LEFT, KEY, left1, right1)
        )
        .addEqualityGroup(
            new TableTableJoin<>(props1, INNER, KEY2, left1, right1)
        )
        .addEqualityGroup(
            new TableTableJoin<>(props1, INNER, KEY, left2, right1)
        )
        .addEqualityGroup(
            new TableTableJoin<>(props1, INNER, KEY, left1, right2)
        ).testEquals();
  }
}