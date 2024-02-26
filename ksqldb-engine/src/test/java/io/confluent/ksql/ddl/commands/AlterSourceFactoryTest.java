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

package io.confluent.ksql.ddl.commands;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import io.confluent.ksql.execution.ddl.commands.AlterSourceCommand;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.AlterOption;
import io.confluent.ksql.parser.tree.AlterSource;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Arrays;
import java.util.List;

import io.confluent.ksql.util.KsqlException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AlterSourceFactoryTest {
  private static final SourceName STREAM_NAME = SourceName.of("streamname");
  private static final SourceName TABLE_NAME = SourceName.of("tablename");
  private static final List<AlterOption> NEW_COLUMNS = Arrays.asList(
      new AlterOption("FOO", new Type(SqlTypes.STRING)));

  @Mock
  private MetaStore metaStore;
  @Mock
  KsqlStream ksqlStream;
  @Mock
  KsqlTable ksqlTable;

  private AlterSourceFactory alterSourceFactory;

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    when(ksqlStream.isSource()).thenReturn(false);
    when(ksqlTable.isSource()).thenReturn(false);

    when(metaStore.getSource(STREAM_NAME)).thenReturn(ksqlStream);
    when(metaStore.getSource(TABLE_NAME)).thenReturn(ksqlTable);

    alterSourceFactory = new AlterSourceFactory(metaStore);
  }

  @Test
  public void shouldCreateCommandForAlterStream() {
    // Given:
    final AlterSource alterSource = new AlterSource(STREAM_NAME, DataSourceType.KSTREAM, NEW_COLUMNS);

    // When:
    final AlterSourceCommand result = alterSourceFactory.create(alterSource);

    // Then:
    assertEquals(result.getKsqlType(), DataSourceType.KSTREAM.getKsqlType());
    assertEquals(result.getSourceName(), STREAM_NAME);
    assertEquals(result.getNewColumns().size(), 1);
  }

  @Test
  public void shouldCreateCommandForAlterTable() {
    // Given:
    final AlterSource alterSource = new AlterSource(TABLE_NAME, DataSourceType.KTABLE, NEW_COLUMNS);

    // When:
    final AlterSourceCommand result = alterSourceFactory.create(alterSource);

    // Then:
    assertEquals(result.getKsqlType(), DataSourceType.KTABLE.getKsqlType());
    assertEquals(result.getSourceName(), TABLE_NAME);
    assertEquals(result.getNewColumns().size(), 1);
  }

  @Test
  public void shouldThrowInAlterOnSourceStream() {
    // Given:
    final AlterSource alterSource = new AlterSource(STREAM_NAME, DataSourceType.KSTREAM, NEW_COLUMNS);
    when(ksqlStream.isSource()).thenReturn(true);

    // When:
    final Exception e = assertThrows(
        KsqlException.class, () -> alterSourceFactory.create(alterSource));

    // Then:
    assertThat(e.getMessage(),
        containsString(
            "Cannot alter stream 'streamname': ALTER operations are not supported on "
                + "source streams."));
  }

  @Test
  public void shouldThrowInAlterOnSourceTable() {
    // Given:
    final AlterSource alterSource = new AlterSource(TABLE_NAME, DataSourceType.KTABLE, NEW_COLUMNS);
    when(ksqlTable.isSource()).thenReturn(true);

    // When:
    final Exception e = assertThrows(
        KsqlException.class, () -> alterSourceFactory.create(alterSource));

    // Then:
    assertThat(e.getMessage(),
        containsString(
            "Cannot alter table 'tablename': ALTER operations are not supported on "
                + "source tables."));
  }
}
