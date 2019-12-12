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
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConfiguredKsqlPlanTest {
  private static final Map<String, Object> OVERRIDES = Collections.singletonMap("foo", "bar");
  private static final Map<String, Object> OVERRIDES2 = Collections.singletonMap("biz", "baz");

  @Mock
  private KsqlPlan plan;
  @Mock
  private KsqlPlan plan2;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private KsqlConfig ksqlConfig2;

  @Test
  public void testEquality() {
    new EqualsTester()
        .addEqualityGroup(
            ConfiguredKsqlPlan.of(plan, OVERRIDES, ksqlConfig),
            ConfiguredKsqlPlan.of(plan, OVERRIDES, ksqlConfig))
        .addEqualityGroup(ConfiguredKsqlPlan.of(plan2, OVERRIDES, ksqlConfig))
        .addEqualityGroup(ConfiguredKsqlPlan.of(plan, OVERRIDES, ksqlConfig2))
        .addEqualityGroup(ConfiguredKsqlPlan.of(plan, OVERRIDES2, ksqlConfig))
        .testEquals();
  }
}