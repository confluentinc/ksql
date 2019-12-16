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

import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.util.TestExecutionStepSerializationUtil;
import io.confluent.ksql.execution.util.TestExecutionSteps;
import io.confluent.ksql.execution.util.TestPlanJsonMapper;
import java.io.IOException;
import org.junit.Test;

public class TableTableJoinTest {
  private static final TestPlanJsonMapper MAPPER = new TestPlanJsonMapper();
  private static final QueryContext CTX = new QueryContext.Stacker().push("foo").getQueryContext();
  private static final ExecutionStepPropertiesV1 PROPERTIES = new ExecutionStepPropertiesV1(CTX);

  static {
    TestExecutionSteps.register(MAPPER);
  }

  @Test
  public void shouldSerializeDeserializeBasic() throws IOException {
    TestExecutionStepSerializationUtil.shouldSerialize(
        "basic",
        new TableTableJoin<>(
            PROPERTIES,
            JoinType.INNER,
            TestExecutionSteps.TABLE_STEP,
            TestExecutionSteps.TABLE_STEP
        ),
        MAPPER.getMapper()
    );
  }
}
