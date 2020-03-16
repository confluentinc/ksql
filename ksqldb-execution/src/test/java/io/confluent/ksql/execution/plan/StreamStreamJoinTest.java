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

import com.google.common.testing.EqualsTester;
import java.time.Duration;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StreamStreamJoinTest {
  @Mock
  private ExecutionStepPropertiesV1 properties1;
  @Mock
  private ExecutionStepPropertiesV1 properties2;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> left1;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> right1;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> left2;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> right2;
  @Mock
  private Formats leftFormats1;
  @Mock
  private Formats leftFormats2;
  @Mock
  private Formats rightFormats1;
  @Mock
  private Formats rightFormats2;

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new StreamStreamJoin<>(
                properties1,
                JoinType.INNER,
                leftFormats1,
                rightFormats1,
                left1,
                right1,
                Duration.ofSeconds(10),
                Duration.ofSeconds(20)),
            new StreamStreamJoin<>(
                properties1,
                JoinType.INNER,
                leftFormats1,
                rightFormats1,
                left1,
                right1,
                Duration.ofSeconds(10),
                Duration.ofSeconds(20)))
        .addEqualityGroup(
            new StreamStreamJoin<>(
                properties2,
                JoinType.INNER,
                leftFormats1,
                rightFormats1,
                left1,
                right1,
                Duration.ofSeconds(10),
                Duration.ofSeconds(20)))
        .addEqualityGroup(
            new StreamStreamJoin<>(
                properties1,
                JoinType.LEFT,
                leftFormats1,
                rightFormats1,
                left1,
                right1,
                Duration.ofSeconds(10),
                Duration.ofSeconds(20)))
        .addEqualityGroup(
            new StreamStreamJoin<>(
                properties1,
                JoinType.INNER,
                leftFormats2,
                rightFormats1,
                left1,
                right1,
                Duration.ofSeconds(10),
                Duration.ofSeconds(20)))
        .addEqualityGroup(
            new StreamStreamJoin<>(
                properties1,
                JoinType.INNER,
                leftFormats1,
                rightFormats2,
                left1,
                right1,
                Duration.ofSeconds(10),
                Duration.ofSeconds(20)))
        .addEqualityGroup(
            new StreamStreamJoin<>(
                properties1,
                JoinType.INNER,
                leftFormats1,
                rightFormats1,
                left2,
                right1,
                Duration.ofSeconds(10),
                Duration.ofSeconds(20)))
        .addEqualityGroup(
            new StreamStreamJoin<>(
                properties1,
                JoinType.INNER,
                leftFormats1,
                rightFormats1,
                left1,
                right2,
                Duration.ofSeconds(10),
                Duration.ofSeconds(20)))
        .addEqualityGroup(
            new StreamStreamJoin<>(
                properties1,
                JoinType.INNER,
                leftFormats1,
                rightFormats1,
                left1,
                right1,
                Duration.ofSeconds(11),
                Duration.ofSeconds(20)))
        .addEqualityGroup(
            new StreamStreamJoin<>(
                properties1,
                JoinType.INNER,
                leftFormats1,
                rightFormats1,
                left1,
                right1,
                Duration.ofSeconds(10),
                Duration.ofSeconds(21)));
  }
}