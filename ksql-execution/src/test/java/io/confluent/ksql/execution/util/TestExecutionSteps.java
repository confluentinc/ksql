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

package io.confluent.ksql.execution.util;

import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KGroupedStreamHolder;
import io.confluent.ksql.execution.plan.KGroupedTableHolder;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.StreamGroupBy;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.TableGroupBy;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import java.util.Collections;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;

public class TestExecutionSteps {
  private static final LogicalSchema SCHEMA = mock(LogicalSchema.class);
  private static final QueryContext CTX
      = new QueryContext.Stacker().push("ctx").getQueryContext();
  private static final ExecutionStepPropertiesV1 PROPERTIES = new ExecutionStepPropertiesV1(CTX);
  private static final Formats FORMATS = Formats.of(
      FormatInfo.of(Format.KAFKA),
      FormatInfo.of(Format.JSON),
      Collections.emptySet()
  );

  public static ExecutionStep<KStreamHolder<Struct>> STREAM_STEP = new StreamSource(
      PROPERTIES,
      "topic",
      FORMATS,
      Optional.empty(),
      SCHEMA,
      SourceName.of("source")
  );

  public static ExecutionStep<KTableHolder<Struct>> TABLE_STEP = new TableSource(
      PROPERTIES,
      "topic",
      FORMATS,
      Optional.empty(),
      SCHEMA,
      SourceName.of("source")
  );

  public static ExecutionStep<KGroupedStreamHolder> GROUPED_STREAM_STEP = new StreamGroupBy<>(
      PROPERTIES,
      STREAM_STEP,
      FORMATS,
      ImmutableList.of()
  );

  public static ExecutionStep<KGroupedTableHolder> GROUPED_TABLE_STEP = new TableGroupBy<>(
      PROPERTIES,
      TABLE_STEP,
      FORMATS,
      ImmutableList.of()
  );

  public static void register(final TestPlanJsonMapper mapper) {
    mapper.register(SCHEMA, "src_schema");
  }
}
