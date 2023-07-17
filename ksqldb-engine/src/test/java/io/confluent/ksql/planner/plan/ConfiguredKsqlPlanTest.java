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

package io.confluent.ksql.planner.plan;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlPlan;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConfiguredKsqlPlanTest {

  @Mock
  private KsqlPlan plan;
  @Mock
  private KsqlPlan plan2;
  @Mock
  private SessionConfig config;
  @Mock
  private SessionConfig config2;

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void testEquality() {
    new EqualsTester()
        .addEqualityGroup(
            ConfiguredKsqlPlan.of(plan, config),
            ConfiguredKsqlPlan.of(plan, config)
        )
        .addEqualityGroup(ConfiguredKsqlPlan.of(plan2, config2))
        .testEquals();
  }
}