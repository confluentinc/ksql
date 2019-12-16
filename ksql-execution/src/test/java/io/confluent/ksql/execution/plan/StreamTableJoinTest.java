package io.confluent.ksql.execution.plan;

import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.util.TestExecutionStepSerializationUtil;
import io.confluent.ksql.execution.util.TestExecutionSteps;
import io.confluent.ksql.execution.util.TestPlanJsonMapper;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeOption;
import java.io.IOException;
import org.junit.Test;

public class StreamTableJoinTest {
  private static final TestPlanJsonMapper MAPPER = new TestPlanJsonMapper();
  private static final QueryContext CTX = new QueryContext.Stacker().push("foo").getQueryContext();
  private static final ExecutionStepPropertiesV1 PROPERTIES = new ExecutionStepPropertiesV1(CTX);
  private static final Formats LEFT_FMT = Formats.of(
      FormatInfo.of(Format.JSON),
      FormatInfo.of(Format.AVRO),
      SerdeOption.none()
  );

  static {
    TestExecutionSteps.register(MAPPER);
  }

  @Test
  public void shouldSerializeDeserializeBasic() throws IOException {
    TestExecutionStepSerializationUtil.shouldSerialize(
        "basic",
        new StreamTableJoin<>(
            PROPERTIES,
            JoinType.INNER,
            LEFT_FMT,
            TestExecutionSteps.STREAM_STEP,
            TestExecutionSteps.TABLE_STEP
        ),
        MAPPER.getMapper()
    );
  }
}