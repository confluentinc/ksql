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

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.windows.HoppingWindowExpression;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.serde.WindowInfo;
import java.time.Duration;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HoppingWindowExpressionTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new HoppingWindowExpression(
                new WindowTimeClause(10, SECONDS),
                new WindowTimeClause(20, MINUTES)),
            new HoppingWindowExpression(
                new WindowTimeClause(10, SECONDS),
                new WindowTimeClause( 20, MINUTES)),
            new HoppingWindowExpression(
                Optional.of(SOME_LOCATION),
                new WindowTimeClause(10, SECONDS),
                new WindowTimeClause(20, MINUTES),
                Optional.empty(),
                Optional.empty()),
            new HoppingWindowExpression(
                Optional.of(OTHER_LOCATION),
                new WindowTimeClause(10, SECONDS),
                new WindowTimeClause(20, MINUTES),
                Optional.empty(),
                Optional.empty())
        )
        .addEqualityGroup(
            new HoppingWindowExpression(
                new WindowTimeClause(30, SECONDS),
                new WindowTimeClause(20, MINUTES))
        )
        .addEqualityGroup(
            new HoppingWindowExpression(
                new WindowTimeClause(10, HOURS),
                new WindowTimeClause(20, MINUTES))
        )
        .addEqualityGroup(
            new HoppingWindowExpression(
                new WindowTimeClause(10, SECONDS),
                new WindowTimeClause(1, MINUTES))
        )
        .addEqualityGroup(
            new HoppingWindowExpression(
                new WindowTimeClause(10, SECONDS),
                new WindowTimeClause(20, MILLISECONDS))
        ).addEqualityGroup(
            new HoppingWindowExpression(
                Optional.of(SOME_LOCATION),
                new WindowTimeClause(10, SECONDS),
                new WindowTimeClause(20, MILLISECONDS),
                Optional.of(new WindowTimeClause(40, MINUTES)),
                Optional.of(new WindowTimeClause(0, MINUTES))),
            new HoppingWindowExpression(
                Optional.of(OTHER_LOCATION),
                new WindowTimeClause(10, SECONDS),
                new WindowTimeClause(20, MILLISECONDS),
                Optional.of(new WindowTimeClause(40, MINUTES)),
                Optional.of(new WindowTimeClause(0, MINUTES)))
        )
        .testEquals();
  }

  @Test
  public void shouldReturnWindowInfo() {
    assertThat(new HoppingWindowExpression(
            new WindowTimeClause(10, SECONDS),
            new WindowTimeClause(20, MINUTES)
        ).getWindowInfo(),
        is(WindowInfo.of(WindowType.HOPPING, Optional.of(Duration.ofSeconds(10)), Optional.empty())));
  }
}