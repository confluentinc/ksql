/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.engine;

import com.google.common.collect.ImmutableSet;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;

import java.util.Optional;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("UnstableApiUsage")
@RunWith(MockitoJUnitRunner.class)
public class QueryPlanTest {
  @Mock
  private SourceName sink1;
  @Mock
  private SourceName sink2;
  @Mock
  private ExecutionStep<?> plan1;
  @Mock
  private ExecutionStep<?> plan2;
  @Mock
  private QueryId id1;
  @Mock
  private QueryId id2;

  private Set<SourceName> sources1;
  private Set<SourceName> sources2;

  @Before
  public void setup() {
    sources1 = ImmutableSet.of(SourceName.of("1"));
    sources2 = ImmutableSet.of(SourceName.of("2"));
  }

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new QueryPlan(sources1, Optional.of(sink1), plan1, id1, Optional.empty()),
            new QueryPlan(sources1, Optional.of(sink1), plan1, id1, Optional.empty()))
        .addEqualityGroup(new QueryPlan(sources2, Optional.of(sink1), plan1, id1, Optional.empty()))
        .addEqualityGroup(new QueryPlan(sources1, Optional.of(sink2), plan1, id1, Optional.empty()))
        .addEqualityGroup(new QueryPlan(sources1, Optional.of(sink1), plan2, id1, Optional.empty()))
        .addEqualityGroup(new QueryPlan(sources1, Optional.of(sink1), plan1, id2, Optional.empty()))
        .testEquals();
  }
}