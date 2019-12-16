package io.confluent.ksql.execution.plan;

import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.util.TestExecutionStepSerializationUtil;
import io.confluent.ksql.execution.util.TestExecutionSteps;
import io.confluent.ksql.execution.util.TestPlanJsonMapper;
import java.io.IOException;
import org.junit.Test;

public class StreamFlatMapTest {
  private static final TestPlanJsonMapper MAPPER = new TestPlanJsonMapper();
  private static final FunctionCall FUNCTION1 = mock(FunctionCall.class);
  private static final FunctionCall FUNCTION2 = mock(FunctionCall.class);
  private static final QueryContext CTX = new QueryContext.Stacker().push("foo").getQueryContext();
  private static final ExecutionStepPropertiesV1 PROPERTIES = new ExecutionStepPropertiesV1(CTX);

  static {
    MAPPER.register(FUNCTION1, "function1");
    MAPPER.register(FUNCTION2, "function2");
    TestExecutionSteps.register(MAPPER);
  }

  @Test
  public void shouldSerializeDeserializeBasic() throws IOException {
    TestExecutionStepSerializationUtil.shouldSerialize(
        "basic",
        new StreamFlatMap<>(
            PROPERTIES,
            TestExecutionSteps.STREAM_STEP,
            ImmutableList.of(FUNCTION1, FUNCTION2)
        ),
        MAPPER.getMapper()
    );
  }
}