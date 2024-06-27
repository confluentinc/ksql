/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class AssertTopicTest {

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private final String SOME_TOPIC = "TOPIC";
  private final Map<String, Literal> SOME_CONFIG = ImmutableMap.of("partitions", new IntegerLiteral(1));
  private final WindowTimeClause SOME_TIMEOUT = new WindowTimeClause(5, TimeUnit.SECONDS);

  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new AssertTopic(Optional.empty(), SOME_TOPIC, SOME_CONFIG, Optional.of(SOME_TIMEOUT), true),
            new AssertTopic(Optional.of(new NodeLocation(1, 1)), SOME_TOPIC, SOME_CONFIG, Optional.of(SOME_TIMEOUT), true))
        .addEqualityGroup(
            new AssertTopic(Optional.empty(), "another topic", SOME_CONFIG, Optional.of(SOME_TIMEOUT), true))
        .addEqualityGroup(
            new AssertTopic(Optional.empty(), SOME_TOPIC, ImmutableMap.of(), Optional.of(SOME_TIMEOUT), true))
        .addEqualityGroup(
            new AssertTopic(Optional.empty(), SOME_TOPIC, SOME_CONFIG, Optional.empty(), true))
        .addEqualityGroup(
            new AssertTopic(Optional.empty(), SOME_TOPIC, SOME_CONFIG, Optional.of(SOME_TIMEOUT), false))
        .testEquals();
  }

}
