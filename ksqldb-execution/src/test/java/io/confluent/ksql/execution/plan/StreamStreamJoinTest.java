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

import static io.confluent.ksql.execution.plan.JoinType.INNER;
import static io.confluent.ksql.execution.plan.JoinType.LEFT;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.name.ColumnName;
import java.time.Duration;
import java.util.Optional;

import org.apache.kafka.connect.data.Struct;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StreamStreamJoinTest {

  private static final ColumnName KEY = ColumnName.of("Bob");
  private static final ColumnName KEY2 = ColumnName.of("Vic");

  private static final Duration TWO_SEC = Duration.ofSeconds(2);
  private static final Duration SIX_SEC = Duration.ofSeconds(6);
  private static final Duration TEN_SEC = Duration.ofSeconds(10);

  @Mock
  private ExecutionStepPropertiesV1 props;
  @Mock
  private ExecutionStepPropertiesV1 props2;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> left;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> right;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> left2;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> right2;
  @Mock
  private Formats lFmts;
  @Mock
  private Formats rFmts;

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new StreamStreamJoin<>(props, INNER, KEY, lFmts, rFmts,
                left, right, TEN_SEC, SIX_SEC, Optional.empty()),
            new StreamStreamJoin<>(props, INNER, KEY, lFmts, rFmts,
                left, right, TEN_SEC, SIX_SEC, Optional.empty())
        )
        .addEqualityGroup(
            new StreamStreamJoin<>(props2, INNER, KEY, lFmts, rFmts,
                left, right, TEN_SEC, SIX_SEC, Optional.empty())
        )
        .addEqualityGroup(
            new StreamStreamJoin<>(props, LEFT, KEY, lFmts, rFmts,
                left, right, TEN_SEC, SIX_SEC, Optional.empty())
        )
        .addEqualityGroup(
            new StreamStreamJoin<>(props, LEFT, KEY2, lFmts, rFmts,
                left, right, TEN_SEC, SIX_SEC, Optional.empty())
        )
        .addEqualityGroup(
            new StreamStreamJoin<>(props, INNER, KEY, rFmts, rFmts,
                left, right, TEN_SEC, SIX_SEC, Optional.empty())
        )
        .addEqualityGroup(
            new StreamStreamJoin<>(props, INNER, KEY, lFmts, lFmts,
                left, right, TEN_SEC, SIX_SEC, Optional.empty())
        )
        .addEqualityGroup(
            new StreamStreamJoin<>(props, INNER, KEY, lFmts, rFmts,
                left2, right, TEN_SEC, SIX_SEC, Optional.empty())
        )
        .addEqualityGroup(
            new StreamStreamJoin<>(props, INNER, KEY, lFmts, rFmts,
                left, right2, TEN_SEC, SIX_SEC, Optional.empty())
        )
        .addEqualityGroup(
            new StreamStreamJoin<>(props, INNER, KEY, lFmts, rFmts,
                left, right, SIX_SEC, SIX_SEC, Optional.empty())
        )
        .addEqualityGroup(
            new StreamStreamJoin<>(props, INNER, KEY, lFmts, rFmts,
                left, right, TEN_SEC, TEN_SEC, Optional.empty())
        )
        .addEqualityGroup(
            new StreamStreamJoin<>(props, INNER, KEY, lFmts, rFmts,
                left, right, TEN_SEC, SIX_SEC, Optional.of(TWO_SEC))
        ).testEquals();
  }
}