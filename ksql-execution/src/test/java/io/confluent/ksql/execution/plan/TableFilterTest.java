package io.confluent.ksql.execution.plan;

import static org.mockito.Mockito.mock;

import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.util.TestExecutionStepSerializationUtil;
import io.confluent.ksql.execution.util.TestExecutionSteps;
import io.confluent.ksql.execution.util.TestPlanJsonMapper;
import java.io.IOException;
import org.junit.Test;

public class TableFilterTest {
  private static final TestPlanJsonMapper MAPPER = new TestPlanJsonMapper();
  private static final Expression EXPRESSION = mock(Expression.class);
  private static final QueryContext CTX = new QueryContext.Stacker().push("foo").getQueryContext();
  private static final ExecutionStepPropertiesV1 PROPERTIES = new ExecutionStepPropertiesV1(CTX);

  static {
    MAPPER.register(EXPRESSION, "filter_expression");
    TestExecutionSteps.register(MAPPER);
  }

  @Test
  public void shouldSerializeBasic() throws IOException {
    TestExecutionStepSerializationUtil.shouldSerialize(
        "basic",
        new TableFilter<>(
            PROPERTIES,
            TestExecutionSteps.TABLE_STEP,
            EXPRESSION
        ),
        MAPPER.getMapper()
    );
  }
}