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

package io.confluent.ksql.execution.ddl.commands;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.junit.Test;

public class CreateSourceCommandTest {

  private static final SourceName SOURCE_NAME = SourceName.of("bob");
  private static final String TOPIC_NAME = "vic";
  private static final Formats FORAMTS = mock(Formats.class);
  private static final ColumnName K0 = ColumnName.of("k0");
  private static final ColumnName K1 = ColumnName.of("k1");


  @Test(expected = UnsupportedOperationException.class)
  public void shouldThrowOnMultipleKeyColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .keyColumn(K0, SqlTypes.STRING)
        .keyColumn(K1, SqlTypes.STRING)
        .valueColumn(ColumnName.of("V0"), SqlTypes.STRING)
        .build();

    // When:
    new TestCommand(
        SOURCE_NAME,
        schema,
        Optional.empty(),
        TOPIC_NAME,
        FORAMTS,
        Optional.empty()
    );
  }

  @Test
  public void shouldThrowOnWindowedWithoutKeyColumn() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("V0"), SqlTypes.STRING)
        .build();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> new TestCommand(
            SOURCE_NAME,
            schema,
            Optional.empty(),
            TOPIC_NAME,
            FORAMTS,
            Optional.of(mock(WindowInfo.class))
        )
    );

    // Then:
    assertThat(e.getMessage(), is(("Windowed sources require a key column.")));
  }

  @Test
  public void shouldThrowOnWindowStartColumn() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("K0"), SqlTypes.INTEGER)
        .valueColumn(SystemColumns.WINDOWSTART_NAME, SqlTypes.INTEGER)
        .build();

    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> new TestCommand(
            SOURCE_NAME,
            schema,
            Optional.empty(),
            TOPIC_NAME,
            FORAMTS,
            Optional.empty()
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Schema contains system columns in value schema"));
  }

  @Test
  public void shouldThrowOnWindowEndColumn() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k1"), SqlTypes.INTEGER)
        .valueColumn(SystemColumns.WINDOWEND_NAME, SqlTypes.INTEGER)
        .build();

    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> new TestCommand(
            SOURCE_NAME,
            schema,
            Optional.empty(),
            TOPIC_NAME,
            FORAMTS,
            Optional.empty()
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Schema contains system columns in value schema"));
  }

  private static final class TestCommand extends CreateSourceCommand {

    TestCommand(
        final SourceName sourceName,
        final LogicalSchema schema,
        final Optional<TimestampColumn> timestampColumn,
        final String topicName,
        final Formats formats,
        final Optional<WindowInfo> windowInfo
    ) {
      super(sourceName, schema, timestampColumn, topicName, formats, windowInfo, false);
    }

    @Override
    public DdlCommandResult execute(final Executor executor) {
      return null;
    }
  }
}