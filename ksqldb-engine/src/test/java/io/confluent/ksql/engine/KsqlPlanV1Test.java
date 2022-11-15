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

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("UnstableApiUsage")
@RunWith(MockitoJUnitRunner.class)
public class KsqlPlanV1Test {
  @Mock
  private DdlCommand ddlCommand1;
  @Mock
  private DdlCommand ddlCommand2;
  @Mock
  private QueryPlan queryPlan1;
  @Mock
  private QueryPlan queryPlan2;

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new KsqlPlanV1("foo", Optional.of(ddlCommand1), Optional.of(queryPlan1)),
            new KsqlPlanV1("foo", Optional.of(ddlCommand1), Optional.of(queryPlan1)),
            // statementText is not checked as part of equals
            new KsqlPlanV1("bar", Optional.of(ddlCommand1), Optional.of(queryPlan1)))
        .addEqualityGroup(new KsqlPlanV1("foo", Optional.of(ddlCommand2), Optional.of(queryPlan1)))
        .addEqualityGroup(new KsqlPlanV1("foo", Optional.of(ddlCommand1), Optional.of(queryPlan2)))
        .testEquals();
  }
}