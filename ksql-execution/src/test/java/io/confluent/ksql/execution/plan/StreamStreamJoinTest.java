package io.confluent.ksql.execution.plan;

import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.util.TestExecutionStepSerializationUtil;
import io.confluent.ksql.execution.util.TestExecutionSteps;
import io.confluent.ksql.execution.util.TestPlanJsonMapper;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeOption;
import java.io.IOException;
import java.time.Duration;
import org.junit.Test;

public class StreamStreamJoinTest {
  private static final TestPlanJsonMapper MAPPER = new TestPlanJsonMapper();
  private static final QueryContext CTX = new QueryContext.Stacker().push("foo").getQueryContext();
  private static final ExecutionStepPropertiesV1 PROPERTIES = new ExecutionStepPropertiesV1(CTX);
  private static final Formats LEFT_FMT = Formats.of(
      FormatInfo.of(Format.JSON),
      FormatInfo.of(Format.AVRO),
      SerdeOption.none()
  );
  private static final Formats RIGHT_FMT = Formats.of(
      FormatInfo.of(Format.KAFKA),
      FormatInfo.of(Format.JSON),
      SerdeOption.none()
  );

  static {
    TestExecutionSteps.register(MAPPER);
  }

  @Test
  public void shouldSerializeDeserializeBasic() throws IOException {
    TestExecutionStepSerializationUtil.shouldSerialize(
        "basic",
        new StreamStreamJoin<>(
            PROPERTIES,
            JoinType.INNER,
            LEFT_FMT,
            RIGHT_FMT,
            TestExecutionSteps.STREAM_STEP,
            TestExecutionSteps.STREAM_STEP,
            Duration.ofSeconds(30),
            Duration.ofSeconds(60)
        ),
        MAPPER.getMapper()
    );
  }
}