package io.confluent.ksql.execution.plan;


import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.util.TestExecutionStepSerializationUtil;
import io.confluent.ksql.execution.util.TestExecutionSteps;
import io.confluent.ksql.execution.util.TestPlanJsonMapper;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import java.io.IOException;
import java.util.Collections;
import org.junit.Test;

public class StreamGroupByKeyTest {
  private static final TestPlanJsonMapper MAPPER = new TestPlanJsonMapper();
  private static final QueryContext CTX = new QueryContext.Stacker().push("foo").getQueryContext();
  private static final ExecutionStepPropertiesV1 PROPERTIES = new ExecutionStepPropertiesV1(CTX);
  private static final Formats FORMATS = Formats.of(
      FormatInfo.of(Format.KAFKA),
      FormatInfo.of(Format.JSON),
      Collections.emptySet()
  );

  static {
    TestExecutionSteps.register(MAPPER);
  }

  @Test
  public void shouldSerializeDeserializeBasic() throws IOException {
    TestExecutionStepSerializationUtil.shouldSerialize(
        "basic",
        new StreamGroupByKey(
            PROPERTIES,
            TestExecutionSteps.STREAM_STEP,
            FORMATS
        ),
        MAPPER.getMapper()
    );
  }
}