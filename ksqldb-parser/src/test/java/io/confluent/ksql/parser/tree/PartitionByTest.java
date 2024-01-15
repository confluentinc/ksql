/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PartitionByTest {

  private static final NodeLocation LOCATION = new NodeLocation(1, 4);

  @Mock
  private Expression exp1;
  @Mock
  private Expression exp2;

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            new PartitionBy(Optional.empty(), ImmutableList.of(exp1)),
            new PartitionBy(Optional.empty(), ImmutableList.of(exp1)),
            new PartitionBy(Optional.of(LOCATION), ImmutableList.of(exp1))
        )
        .addEqualityGroup(
            new PartitionBy(Optional.empty(), ImmutableList.of(exp2))
        )
        .testEquals();
  }
}