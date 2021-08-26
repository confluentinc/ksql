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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.execution.ddl.commands.CreateTableCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import java.util.Optional;

import io.confluent.ksql.util.KsqlConstants;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
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

  @Test
  public void shouldReturnNoPersistentQueryTypeOnPlansWithoutQueryPlans() {
    // Given:
    final KsqlPlanV1 plan = new KsqlPlanV1(
        "stmt",
        Optional.of(ddlCommand1),
        Optional.empty());

    // When/Then:
    assertThat(plan.getPersistentQueryType(), is(Optional.empty()));
  }

  @Test
  public void shouldReturnInsertPersistentQueryTypeOnPlansWithoutDdlCommands() {
    // Given:
    final KsqlPlanV1 plan = new KsqlPlanV1(
        "stmt",
        Optional.empty(),
        Optional.of(queryPlan1));

    // When/Then:
    assertThat(plan.getPersistentQueryType(),
        is(Optional.of(KsqlConstants.PersistentQueryType.INSERT)));
  }

  @Test
  public void shouldReturnCreateSourcePersistentQueryTypeOnCreateSourceTable() {
    // Given:
    final CreateTableCommand ddlCommand = Mockito.mock(CreateTableCommand.class);
    when(ddlCommand.getIsSource()).thenReturn(true);
    final KsqlPlanV1 plan = new KsqlPlanV1(
        "stmt",
        Optional.of(ddlCommand),
        Optional.of(queryPlan1));

    // When/Then:
    assertThat(plan.getPersistentQueryType(),
        is(Optional.of(KsqlConstants.PersistentQueryType.CREATE_SOURCE)));
  }

  @Test
  public void shouldReturnCreateAsPersistentQueryTypeOnCreateTable() {
    // Given:
    final CreateTableCommand ddlCommand = Mockito.mock(CreateTableCommand.class);
    when(ddlCommand.getIsSource()).thenReturn(false);
    final KsqlPlanV1 plan = new KsqlPlanV1(
        "stmt",
        Optional.of(ddlCommand),
        Optional.of(queryPlan1));

    // When/Then:
    assertThat(plan.getPersistentQueryType(),
        is(Optional.of(KsqlConstants.PersistentQueryType.CREATE_AS)));
  }

  @Test
  public void shouldReturnCreateAsPersistentQueryTypeOnCreateStream() {
    // Given:
    final CreateStreamCommand ddlCommand = Mockito.mock(CreateStreamCommand.class);
    final KsqlPlanV1 plan = new KsqlPlanV1(
        "stmt",
        Optional.of(ddlCommand),
        Optional.of(queryPlan1));

    // When/Then:
    assertThat(plan.getPersistentQueryType(),
        is(Optional.of(KsqlConstants.PersistentQueryType.CREATE_AS)));
  }
}