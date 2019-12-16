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

public class StreamSourceTest {
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
        new StreamSource(
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