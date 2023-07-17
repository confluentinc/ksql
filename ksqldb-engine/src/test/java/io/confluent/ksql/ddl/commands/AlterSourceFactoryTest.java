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

import static org.junit.Assert.assertEquals;

import io.confluent.ksql.execution.ddl.commands.AlterSourceCommand;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.AlterOption;
import io.confluent.ksql.parser.tree.AlterSource;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AlterSourceFactoryTest {
  private static final SourceName STREAM_NAME = SourceName.of("streamname");
  private static final SourceName TABLE_NAME = SourceName.of("tablename");
  private static final List<AlterOption> NEW_COLUMNS = Arrays.asList(
      new AlterOption("FOO", new Type(SqlTypes.STRING)));

  private AlterSourceFactory alterSourceFactory;

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    alterSourceFactory = new AlterSourceFactory();
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
}
