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
  private static final Formats FORMATS = mock(Formats.class);

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
            FORMATS,
            Optional.of(mock(WindowInfo.class))
        )
    );

    // Then:
    assertThat(e.getMessage(), is(("Windowed sources require a key column.")));
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
      super(sourceName, schema, timestampColumn, topicName, formats, windowInfo, false, false);
    }

    @Override
    public DdlCommandResult execute(final Executor executor) {
      return null;
    }
  }
}