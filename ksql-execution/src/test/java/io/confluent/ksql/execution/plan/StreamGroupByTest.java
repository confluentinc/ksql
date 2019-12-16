package io.confluent.ksql.execution.plan;

import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.util.TestExecutionStepSerializationUtil;
import io.confluent.ksql.execution.util.TestExecutionSteps;
import io.confluent.ksql.execution.util.TestPlanJsonMapper;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import java.io.IOException;
import java.util.Collections;
import org.junit.Test;

public class StreamGroupByTest {
  private static final TestPlanJsonMapper MAPPER = new TestPlanJsonMapper();
  private static final Expression EXPRESSION1 = mock(Expression.class);
  private static final Expression EXPRESSION2 = mock(Expression.class);
  private static final QueryContext CTX = new QueryContext.Stacker().push("foo").getQueryContext();
  private static final Formats FMTS =
      Formats.of(FormatInfo.of(Format.KAFKA), FormatInfo.of(Format.JSON), Collections.emptySet());
  private static final ExecutionStepPropertiesV1 PROPERTIES = new ExecutionStepPropertiesV1(CTX);

  static {
    MAPPER.register(EXPRESSION1, "expr1");
    MAPPER.register(EXPRESSION2, "expr2");
    TestExecutionSteps.register(MAPPER);
  }

  @Test
  public void shouldSerializeDeserializeBasic() throws IOException {
    TestExecutionStepSerializationUtil.shouldSerialize(
        "basic",
        new StreamGroupBy<>(
            PROPERTIES,
            TestExecutionSteps.STREAM_STEP,
            FMTS,
            ImmutableList.of(EXPRESSION1, EXPRESSION2)
        ),
        MAPPER.getMapper()
    );
  }
}