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

import static org.mockito.Mockito.mock;

import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CreateSourceCommandTest {

  private static final SourceName SOURCE_NAME = SourceName.of("bob");
  private static final String TOPIC_NAME = "vic";
  private static final Formats FORAMTS = mock(Formats.class);
  private static final ColumnName K0 = ColumnName.of("k0");
  private static final ColumnName K1 = ColumnName.of("k1");
  private static final ColumnName KEY_FIELD = ColumnName.of("keyField");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();


  @Test(expected = UnsupportedOperationException.class)
  public void shouldThrowOnMultipleKeyColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(K0, SqlTypes.STRING)
        .keyColumn(K1, SqlTypes.STRING)
        .build();

    // When:
    new TestCommand(
        SOURCE_NAME,
        schema,
        Optional.empty(),
        Optional.empty(),
        TOPIC_NAME,
        FORAMTS,
        Optional.empty()
    );
  }

  @Test
  public void shouldThrowIfKeyFieldDoesNotMatchRowKeyType() {
    // Given:
    final ColumnName keyField = ColumnName.of("keyField");

    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(K0, SqlTypes.INTEGER)
        .valueColumn(keyField, SqlTypes.STRING)
        .build();

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("The KEY field (keyField) identified in the "
        + "WITH clause is of a different type to the actual key column.");
    expectedException.expectMessage(
        "Either change the type of the KEY field to match ROWKEY, or explicitly set ROWKEY "
            + "to the type of the KEY field by adding 'ROWKEY STRING KEY' in the schema.");
    expectedException.expectMessage("KEY field type: STRING");
    expectedException.expectMessage("ROWKEY type: INTEGER");

    // When:
    new TestCommand(
        SOURCE_NAME,
        schema,
        Optional.of(keyField),
        Optional.empty(),
        TOPIC_NAME,
        FORAMTS,
        Optional.empty()
    );
  }

  @Test
  public void shouldNotThrowIfKeyFieldMatchesRowKeyType() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(K0, SqlTypes.INTEGER)
        .valueColumn(KEY_FIELD, SqlTypes.INTEGER)
        .build();

    // When:
    new TestCommand(
        SOURCE_NAME,
        schema,
        Optional.of(KEY_FIELD),
        Optional.empty(),
        TOPIC_NAME,
        FORAMTS,
        Optional.empty()
    );

    // Then: builds without error
  }

  @Test
  public void shouldThrowOnWindowStartColumn() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.INTEGER)
        .valueColumn(SchemaUtil.WINDOWSTART_NAME, SqlTypes.INTEGER)
        .build();

    // Expect:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Schema contains system columns in value schema");

    // When:
    new TestCommand(
        SOURCE_NAME,
        schema,
        Optional.empty(),
        Optional.empty(),
        TOPIC_NAME,
        FORAMTS,
        Optional.empty()
    );
  }

  @Test
  public void shouldThrowOnWindowEndColumn() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.INTEGER)
        .valueColumn(SchemaUtil.WINDOWEND_NAME, SqlTypes.INTEGER)
        .build();

    // Expect:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Schema contains system columns in value schema");

    // When:
    new TestCommand(
        SOURCE_NAME,
        schema,
        Optional.empty(),
        Optional.empty(),
        TOPIC_NAME,
        FORAMTS,
        Optional.empty()
    );
  }

  private static final class TestCommand extends CreateSourceCommand {

    TestCommand(
        final SourceName sourceName,
        final LogicalSchema schema,
        final Optional<ColumnName> keyField,
        final Optional<TimestampColumn> timestampColumn,
        final String topicName,
        final Formats formats,
        final Optional<WindowInfo> windowInfo
    ) {
      super(sourceName, schema, keyField, timestampColumn, topicName, formats, windowInfo);
    }

    @Override
    public DdlCommandResult execute(final Executor executor) {
      return null;
    }
  }
}