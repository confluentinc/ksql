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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.windows.TumblingWindowExpression;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.OutputRefinement;
import io.confluent.ksql.serde.WindowInfo;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TumblingWindowExpressionTest {
  @Test
  public void shouldReturnWindowInfo() {
    assertThat(new TumblingWindowExpression(new WindowTimeClause(11, SECONDS)).getWindowInfo(),
        is(WindowInfo.of(
            WindowType.TUMBLING,
            Optional.of(Duration.ofSeconds(11)),
            Optional.empty())));
    assertThat(new TumblingWindowExpression(
            Optional.empty(),
            new WindowTimeClause(20, SECONDS),
            Optional.of(new WindowTimeClause(20, SECONDS)),
            Optional.of(new WindowTimeClause(10, SECONDS)),
            Optional.of(OutputRefinement.CHANGES)
        ).getWindowInfo(),
        is(WindowInfo.of(
            WindowType.TUMBLING,
            Optional.of(Duration.ofSeconds(20)),
            Optional.of(OutputRefinement.CHANGES))));
  }

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            new TumblingWindowExpression(
                new WindowTimeClause(50, TimeUnit.SECONDS)
            )
        )
        .addEqualityGroup(
            new TumblingWindowExpression(
                Optional.empty(),
                new WindowTimeClause(40, TimeUnit.SECONDS),
                Optional.of(new WindowTimeClause(80, TimeUnit.SECONDS)),
                Optional.of(new WindowTimeClause(40, TimeUnit.SECONDS))
            ),
            new TumblingWindowExpression(
                Optional.of(new NodeLocation(0,0)),
                new WindowTimeClause(40, TimeUnit.SECONDS),
                Optional.of(new WindowTimeClause(80, TimeUnit.SECONDS)),
                Optional.of(new WindowTimeClause(40, TimeUnit.SECONDS))
            )
        )
        .testEquals();
  }
}