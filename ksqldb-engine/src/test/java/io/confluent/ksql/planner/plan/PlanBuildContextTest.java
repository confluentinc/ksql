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
 *
 */

package io.confluent.ksql.planner.plan;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class PlanBuildContextTest {
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private FunctionRegistry functionRegistry;

  private PlanBuildContext runtimeBuildContext;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setup() {
    runtimeBuildContext = PlanBuildContext.of(
        ksqlConfig,
        serviceContext,
        functionRegistry,
        Optional.empty()
    );
  }

  @Test
  public void shouldBuildNodeContext() {
    // When:
    final Stacker result = runtimeBuildContext.buildNodeContext("some-id");

    // Then:
    assertThat(result, is(new Stacker().push("some-id")));
  }

  @Test
  public void shouldSwapInKsqlConfig() {
    // Given:
    final KsqlConfig other = mock(KsqlConfig.class);

    // When:
    final PlanBuildContext result = runtimeBuildContext.withKsqlConfig(other);

    // Then:
    assertThat(runtimeBuildContext.getKsqlConfig(), is(ksqlConfig));
    assertThat(result.getKsqlConfig(), is(other));
  }
}