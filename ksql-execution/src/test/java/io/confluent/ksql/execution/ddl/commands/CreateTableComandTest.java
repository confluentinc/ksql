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
import io.confluent.ksql.execution.util.TestExecutionStepSerializationUtil;
import io.confluent.ksql.execution.util.TestPlanJsonMapper;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.WindowInfo;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import org.junit.Test;

public class CreateTableComandTest {
  private static final TestPlanJsonMapper MAPPER = new TestPlanJsonMapper();
  private static final ColumnRef COLUMN_REF = mock(ColumnRef.class);
  private static final LogicalSchema SCHEMA = mock(LogicalSchema.class);
  private static final Formats FORMATS = Formats.of(
      FormatInfo.of(Format.JSON),
      FormatInfo.of(Format.AVRO),
      Collections.emptySet()
  );

  static {
    MAPPER.register(COLUMN_REF, "col");
    MAPPER.register(SCHEMA, "schema");
  }

  @Test
  public void shouldSerializeDeserializeBasic() throws IOException {
    TestExecutionStepSerializationUtil.shouldSerialize(
        "basic",
        new CreateTableCommand(
            SourceName.of("table"),
            SCHEMA,
            Optional.of(ColumnName.of("keyfield")),
            Optional.of(new TimestampColumn(COLUMN_REF, Optional.empty())),
            "topic",
            FORMATS,
            Optional.of(WindowInfo.of(WindowType.TUMBLING, Optional.of(Duration.ofSeconds(30))))
        ),
        MAPPER.getMapper()
    );
  }

  @Test
  public void shouldSerializeDeserializeWithoutOptionalFields() throws IOException {
    TestExecutionStepSerializationUtil.shouldSerialize(
        "minimal",
        new CreateTableCommand(
            SourceName.of("table"),
            SCHEMA,
            Optional.empty(),
            Optional.empty(),
            "topic",
            FORMATS,
            Optional.empty()
        ),
        MAPPER.getMapper()
    );
  }
}
