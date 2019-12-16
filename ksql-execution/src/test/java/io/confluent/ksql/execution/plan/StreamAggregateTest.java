package io.confluent.ksql.execution.plan;

import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.util.TestExecutionStepSerializationUtil;
import io.confluent.ksql.execution.util.TestExecutionSteps;
import io.confluent.ksql.execution.util.TestPlanJsonMapper;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import java.io.IOException;
import java.util.Collections;
import org.junit.Test;

public class StreamAggregateTest {
  private static final TestPlanJsonMapper MAPPER = new TestPlanJsonMapper();
  private static final ColumnRef COL1 = mock(ColumnRef.class);
  private static final ColumnRef COL2 = mock(ColumnRef.class);
  private static final FunctionCall AGG1 = mock(FunctionCall.class);
  private static final FunctionCall AGG2 = mock(FunctionCall.class);
  private static final ExecutionStepPropertiesV1 PROPERTIES =
      new ExecutionStepPropertiesV1(new Stacker().push("foo").push("bar").getQueryContext());

  static {
    MAPPER.register(AGG1, "agg1");
    MAPPER.register(AGG2, "agg2");
    MAPPER.register(COL1, "col1");
    MAPPER.register(COL2, "col2");
    TestExecutionSteps.register(MAPPER);
  }

  @Test
  public void shouldSerializeBasic() throws IOException {
    TestExecutionStepSerializationUtil.shouldSerialize(
        "basic",
        new StreamAggregate(
            PROPERTIES,
            TestExecutionSteps.GROUPED_STREAM_STEP,
            Formats.of(FormatInfo.of(Format.JSON), FormatInfo.of(Format.JSON), Collections.emptySet()),
            ImmutableList.of(COL1, COL2),
            ImmutableList.of(AGG1, AGG2)
        ),
        MAPPER.getMapper()
    );
  }
}