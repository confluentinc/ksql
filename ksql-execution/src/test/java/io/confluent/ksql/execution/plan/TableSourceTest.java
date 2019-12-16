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

package io.confluent.ksql.execution.plan;

import static org.mockito.Mockito.mock;

import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.execution.util.TestExecutionStepSerializationUtil;
import io.confluent.ksql.execution.util.TestPlanJsonMapper;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeOption;
import java.io.IOException;
import java.util.Optional;
import org.junit.Test;

public class TableSourceTest {
  private static final TestPlanJsonMapper MAPPER = new TestPlanJsonMapper();
  private static final QueryContext CTX = new QueryContext.Stacker().push("foo").getQueryContext();
  private static final ExecutionStepPropertiesV1 PROPERTIES = new ExecutionStepPropertiesV1(CTX);
  private static final Formats FORMATS = Formats.of(
      FormatInfo.of(Format.JSON),
      FormatInfo.of(Format.AVRO),
      SerdeOption.none()
  );
  private static final ColumnRef COLUMN_REF = mock(ColumnRef.class);
  private static final TimestampColumn TIMESTAMP_COLUMN
      = new TimestampColumn(COLUMN_REF, Optional.empty());
  private static final LogicalSchema SCHEMA = mock(LogicalSchema.class);

  static {
    MAPPER.register(COLUMN_REF, "col");
    MAPPER.register(SCHEMA, "schema");
  }

  @Test
  public void shouldSerializeDeserializeBasic() throws IOException {
    TestExecutionStepSerializationUtil.shouldSerialize(
        "basic",
        new TableSource(
            PROPERTIES,
            "topic",
            FORMATS,
            Optional.of(TIMESTAMP_COLUMN),
            SCHEMA,
            SourceName.of("source")
        ),
        MAPPER.getMapper()
    );
  }
}
