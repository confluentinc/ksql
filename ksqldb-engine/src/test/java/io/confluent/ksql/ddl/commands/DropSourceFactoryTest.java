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

package io.confluent.ksql.ddl.commands;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.execution.ddl.commands.DropSourceCommand;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.util.KsqlException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DropSourceFactoryTest {
  private static final SourceName SOME_NAME = SourceName.of("bob");
  private static final SourceName TABLE_NAME = SourceName.of("tablename");

  @Mock
  private KsqlStream ksqlStream;
  @Mock
  private KsqlTable ksqlTable;
  @Mock
  private MetaStore metaStore;

  private DropSourceFactory dropSourceFactory;

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    when(metaStore.getSource(SOME_NAME)).thenReturn(ksqlStream);
    when(metaStore.getSource(TABLE_NAME)).thenReturn(ksqlTable);
    when(ksqlStream.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(ksqlTable.getDataSourceType()).thenReturn(DataSourceType.KTABLE);

    dropSourceFactory = new DropSourceFactory(metaStore);
  }

  @Test
  public void shouldCreateCommandForDropStream() {
    // Given:
    final DropStream ddlStatement = new DropStream(SOME_NAME, true, true);

    // When:
    final DdlCommand result = dropSourceFactory.create(ddlStatement);

    // Then:
    assertThat(result, instanceOf(DropSourceCommand.class));
  }

  @Test
  public void shouldCreateCommandForDropTable() {
    // Given:
    final DropTable ddlStatement = new DropTable(TABLE_NAME, true, true);

    // When:
    final DdlCommand result = dropSourceFactory.create(ddlStatement);

    // Then:
    assertThat(result, instanceOf(DropSourceCommand.class));
  }

  @Test
  public void shouldCreateDropSourceOnMissingSourceWithIfExistsForStream() {
    // Given:
    final DropStream dropStream = new DropStream(SOME_NAME, true, true);
    when(metaStore.getSource(SOME_NAME)).thenReturn(null);

    // When:
    final DropSourceCommand cmd = dropSourceFactory.create(dropStream);

    // Then:
    assertThat(cmd.getSourceName(), equalTo(SourceName.of("bob")));
  }

  @Test
  public void shouldFailDropSourceOnMissingSourceWithNoIfExistsForStream() {
    // Given:
    final DropStream dropStream = new DropStream(SOME_NAME, false, true);
    when(metaStore.getSource(SOME_NAME)).thenReturn(null);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> dropSourceFactory.create(dropStream)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Stream bob does not exist."));
  }

  @Test
  public void shouldFailDropSourceOnMissingSourceWithNoIfExistsForTable() {
    // Given:
    final DropTable dropTable = new DropTable(SOME_NAME, false, true);
    when(metaStore.getSource(SOME_NAME)).thenReturn(null);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> dropSourceFactory.create(dropTable)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Table bob does not exist."));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldFailDropSourceOnDropIncompatibleSourceForStream() {
    // Given:
    final DropStream dropStream = new DropStream(SOME_NAME, false, true);
    when(metaStore.getSource(SOME_NAME)).thenReturn(ksqlTable);

    // Expect:
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> dropSourceFactory.create(dropStream)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Incompatible data source type is table"));
  }

  @Test
  public void shouldFailDeleteTopicForSourceStream() {
    // Given:
    final DropStream dropStream = new DropStream(SOME_NAME, false, true);
    when(ksqlStream.isSource()).thenReturn(true);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> dropSourceFactory.create(dropStream)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Cannot delete topic for read-only source: bob"));
  }

  @Test
  public void shouldFailDeleteTopicForSourceTable() {
    // Given:
    final DropTable dropTable = new DropTable(TABLE_NAME, false, true);
    when(ksqlTable.isSource()).thenReturn(true);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> dropSourceFactory.create(dropTable)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Cannot delete topic for read-only source: tablename"));
  }
}
