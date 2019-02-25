/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.cli.console.table.builder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.cli.console.Console;
import io.confluent.ksql.cli.console.table.Table;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.util.KsqlConfig;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class PropertiesListTableBuilderTest {

  @Mock
  private Console console;
  @Captor
  private ArgumentCaptor<List<List<String>>> rowsCaptor;
  private PropertiesListTableBuilder builder;
  private static final String SOME_KEY =
      KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

  @Before
  public void setUp() {
    builder = new PropertiesListTableBuilder();
    when(console.writer()).thenReturn(mock(PrintWriter.class));
  }

  @Test
  public void shouldHandleClientOverwrittenProperties() {
    // Given:
    final PropertiesList propList = new PropertiesList("list properties;",
        ImmutableMap.of(SOME_KEY, "earliest"),
        ImmutableList.of(SOME_KEY),
        Collections.emptyList()
    );

    // When:
    final Table table = builder.buildTable(propList);

    // Then:
    assertThat(getRows(table), contains(row(SOME_KEY, "SESSION", "earliest")));
  }

  @Test
  public void shouldHandleServerOverwrittenProperties() {
    // Given:
    final PropertiesList propList = new PropertiesList("list properties;",
        ImmutableMap.of(SOME_KEY, "earliest"),
        Collections.emptyList(),
        Collections.emptyList()
    );

    // When:
    final Table table = builder.buildTable(propList);

    // Then:
    assertThat(getRows(table), contains(row(SOME_KEY, "SERVER", "earliest")));
  }

  @Test
  public void shouldHandleDefaultProperties() {
    // Given:
    final PropertiesList propList = new PropertiesList("list properties;",
        ImmutableMap.of(SOME_KEY, "earliest"),
        Collections.emptyList(),
        ImmutableList.of(SOME_KEY)
    );

    // When:
    final Table table = builder.buildTable(propList);

    // Then:
    assertThat(getRows(table), contains(row(SOME_KEY, "", "earliest")));
  }

  @Test
  public void shouldHandlePropertiesWithNullValue() {
    // Given:
    final PropertiesList propList = new PropertiesList("list properties;",
        Collections.singletonMap(SOME_KEY, null),
        Collections.emptyList(),
        ImmutableList.of(SOME_KEY)
    );

    // When:
    final Table table = builder.buildTable(propList);

    // Then:
    assertThat(getRows(table), contains(row(SOME_KEY, "", "NULL")));
  }

  private List<List<String>> getRows(final Table table) {
    table.print(console);
    verify(console).addResult(rowsCaptor.capture());
    return rowsCaptor.getValue();
  }

  @SuppressWarnings("SameParameterValue")
  private static List<String> row(
      final String property,
      final String defaultValue,
      final String actualValue
  ) {
    return ImmutableList.of(property, defaultValue, actualValue);
  }
}