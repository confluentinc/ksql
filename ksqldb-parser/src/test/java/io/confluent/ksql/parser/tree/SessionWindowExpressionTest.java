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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.windows.SessionWindowExpression;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.serde.WindowInfo;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SessionWindowExpressionTest {

  private SessionWindowExpression windowExpression;

  @Before
  public void setUp() {
    windowExpression = new SessionWindowExpression(new WindowTimeClause(5, TimeUnit.SECONDS));
  }

  @Test
  public void shouldReturnWindowInfo() {
    assertThat(windowExpression.getWindowInfo(),
        is(WindowInfo.of(WindowType.SESSION, Optional.empty(), Optional.empty())));
  }

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            new SessionWindowExpression(
                new WindowTimeClause(10, TimeUnit.SECONDS)
            )
        )
        .addEqualityGroup(
            new SessionWindowExpression(
                Optional.empty(),
                new WindowTimeClause(10, TimeUnit.SECONDS),
                Optional.of(new WindowTimeClause(20, TimeUnit.SECONDS)),
                Optional.of(new WindowTimeClause(0, TimeUnit.SECONDS))
            ),
            new SessionWindowExpression(
                Optional.of(new NodeLocation(0, 0)),
                new WindowTimeClause(10, TimeUnit.SECONDS),
                Optional.of(new WindowTimeClause(20, TimeUnit.SECONDS)),
                Optional.of(new WindowTimeClause(0, TimeUnit.SECONDS))
            )
        )
        .testEquals();
  }
}