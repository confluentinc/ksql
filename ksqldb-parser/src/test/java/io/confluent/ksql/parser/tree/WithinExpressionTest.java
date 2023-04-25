/*
 * Copyright 2018 Confluent Inc.
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

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.confluent.ksql.execution.windows.WindowTimeClause;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.junit.Test;

@SuppressWarnings("deprecation") // can be fixed after GRACE clause is made mandatory
public class WithinExpressionTest {

  @Test
  public void shouldDisplayCorrectStringWithSingleWithin() {
    final WithinExpression expression = new WithinExpression(20, TimeUnit.SECONDS);
    assertEquals(" WITHIN 20 SECONDS", expression.toString());
    assertEquals(
        JoinWindows.of(Duration.ofSeconds(20)),
        expression.joinWindow());
  }

  @Test
  public void shouldDisplayCorrectStringWithBeforeAndAfter() {
    final WithinExpression expression = new WithinExpression(30, 40, TimeUnit.MINUTES, TimeUnit.HOURS);
    assertEquals(" WITHIN (30 MINUTES, 40 HOURS)", expression.toString());
    assertEquals(
        JoinWindows.of(Duration.ofMinutes(30)).after(Duration.ofHours(40)),
        expression.joinWindow());
  }

  @Test
  public void shouldDisplayCorrectStringWithGracePeriod() {
    final WindowTimeClause gracePeriod = new WindowTimeClause(5, TimeUnit.SECONDS);
    final WithinExpression expression = new WithinExpression(20, TimeUnit.SECONDS, gracePeriod);
    assertEquals(" WITHIN 20 SECONDS GRACE PERIOD 5 SECONDS", expression.toString());
    assertEquals(
        JoinWindows.of(Duration.ofSeconds(20)).grace(Duration.ofSeconds(5)),
        expression.joinWindow());
  }

  @Test
  public void shouldDisplayCorrectStringWithBeforeAndAfterWithGracePeriod() {
    final WindowTimeClause gracePeriod = new WindowTimeClause(5, TimeUnit.SECONDS);
    final WithinExpression expression = new WithinExpression(
        30,
        40,
        TimeUnit.MINUTES,
        TimeUnit.HOURS,
        gracePeriod);

    assertEquals(" WITHIN (30 MINUTES, 40 HOURS) GRACE PERIOD 5 SECONDS", expression.toString());
    assertEquals(
        JoinWindows.of(Duration.ofMinutes(30))
            .after(Duration.ofHours(40))
            .grace(Duration.ofSeconds(5)),
        expression.joinWindow());
  }
}
