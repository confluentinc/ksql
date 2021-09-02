/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.statement;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.util.KsqlConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PropertiesInjectorTest {
  @Mock
  private CreateTable createTable;
  @Mock
  private ConfiguredStatement<CreateTable> createTableStatement;
  @Mock
  private SessionConfig sessionConfig;

  private PropertiesInjector propertiesInjector;

  @Before
  public void setup() {
    propertiesInjector = new PropertiesInjector();
    when(createTableStatement.getStatement()).thenReturn(createTable);
    when(createTableStatement.getStatementText()).thenReturn("SQL");
    when(createTableStatement.getSessionConfig()).thenReturn(sessionConfig);
  }

  @Test
  public void shouldSetOffsetResetToEarliestIfNotPresent() {
    // Given:
    final KsqlConfig config = new KsqlConfig(ImmutableMap.of());
    when(createTable.isSource()).thenReturn(true);
    when(sessionConfig.getConfig(false)).thenReturn(config);
    when(sessionConfig.getOverrides()).thenReturn(ImmutableMap.of());

    // When:
    final ConfiguredStatement<CreateTable> newStatement =
        propertiesInjector.inject(createTableStatement);

    // Then:
    assertThat(newStatement.getStatement(), is(createTableStatement.getStatement()));
    assertThat(newStatement.getStatementText(), is(createTableStatement.getStatementText()));
    assertThat(newStatement.getSessionConfig().getConfig(false), is(config));
    assertThat(newStatement.getSessionConfig().getOverrides(),
        is(ImmutableMap.of("auto.offset.reset", "earliest")));
  }

  @Test
  public void shouldSetOffsetResetToEarliestIfOverridden() {
    // Given:
    final KsqlConfig config = new KsqlConfig(ImmutableMap.of());
    when(createTable.isSource()).thenReturn(true);
    when(sessionConfig.getConfig(false)).thenReturn(config);
    when(sessionConfig.getOverrides()).thenReturn(ImmutableMap.of("auto.offset.reset", "latest"));

    // When:
    final ConfiguredStatement<CreateTable> newStatement =
        propertiesInjector.inject(createTableStatement);

    // Then:
    assertThat(newStatement.getStatement(), is(createTableStatement.getStatement()));
    assertThat(newStatement.getStatementText(), is(createTableStatement.getStatementText()));
    assertThat(newStatement.getSessionConfig().getConfig(false), is(config));
    assertThat(newStatement.getSessionConfig().getOverrides(),
        is(ImmutableMap.of("auto.offset.reset", "earliest")));
  }

  @Test
  public void shouldSetOffsetResetToEarliestDifferentSystemConfigIsPresent() {
    // Given:
    final KsqlConfig config = new KsqlConfig(ImmutableMap.of("auto.offset.reset", "latest"));
    when(createTable.isSource()).thenReturn(true);
    when(sessionConfig.getConfig(false)).thenReturn(config);
    when(sessionConfig.getOverrides()).thenReturn(ImmutableMap.of());

    // When:
    final ConfiguredStatement<CreateTable> newStatement =
        propertiesInjector.inject(createTableStatement);

    // Then:
    assertThat(newStatement.getStatement(), is(createTableStatement.getStatement()));
    assertThat(newStatement.getStatementText(), is(createTableStatement.getStatementText()));
    assertThat(newStatement.getSessionConfig().getConfig(false), is(config));
    assertThat(newStatement.getSessionConfig().getOverrides(),
        is(ImmutableMap.of("auto.offset.reset", "earliest")));
  }

  @Test
  public void shouldNotSetOffsetResetOnNoSourceTables() {
    // Given:
    final KsqlConfig config = new KsqlConfig(ImmutableMap.of());
    when(createTable.isSource()).thenReturn(false);
    when(sessionConfig.getConfig(false)).thenReturn(config);
    when(sessionConfig.getOverrides()).thenReturn(ImmutableMap.of());

    // When:
    final ConfiguredStatement<CreateTable> newStatement =
        propertiesInjector.inject(createTableStatement);

    // Then:
    assertThat(newStatement.getStatement(), is(createTableStatement.getStatement()));
    assertThat(newStatement.getStatementText(), is(createTableStatement.getStatementText()));
    assertThat(newStatement.getSessionConfig().getConfig(false), is(config));
    assertThat(newStatement.getSessionConfig().getOverrides(), is(ImmutableMap.of()));
  }
}
