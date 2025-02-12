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
 */

package io.confluent.ksql.rest.server.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.VariablesList;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlHostInfo;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ListVariablesExecutorTest {
  private SessionProperties sessionProperties;

  @Before
  public void setup() {
    sessionProperties = new SessionProperties(
        new HashMap<>(), mock(KsqlHostInfo.class), mock(URL.class), false);
  }

  private Optional<KsqlEntity> executeListVariables(final String sql) {
    final ConfiguredStatement<?> configuredStatement = mock(ConfiguredStatement.class);
    when(configuredStatement.getMaskedStatementText()).thenReturn(sql);

    return CustomExecutors.LIST_VARIABLES.execute(
        configuredStatement,
        sessionProperties,
        null,
        null
    ).getEntity();
  }

  @Test
  public void shouldListEmptyVariables() {
    // When:
    final KsqlEntity response = executeListVariables("LIST VARIABLES;").get();

    // Then:
    assertThat(((VariablesList)response).getVariables(), is(Collections.emptyList()));
  }

  @Test
  public void shouldListVariables() {
    // Given:
    sessionProperties.setVariable("var1", "1");
    sessionProperties.setVariable("var2", "2");

    // When:
    final KsqlEntity response = executeListVariables("LIST VARIABLES;").get();

    // Then:
    final VariablesList variablesList = (VariablesList) response;
    assertThat(variablesList.getStatementText(), is("LIST VARIABLES;"));
    assertThat(variablesList.getVariables().get(0),
        is(new VariablesList.Variable("var1", "1")));
    assertThat(variablesList.getVariables().get(1),
        is(new VariablesList.Variable("var2", "2")));
  }
}
