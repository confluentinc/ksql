/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.plan;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("UnstableApiUsage")
@RunWith(MockitoJUnitRunner.class)
public class TableSourceTest {
  @Mock
  private ExecutionStepPropertiesV1 properties1;
  @Mock
  private ExecutionStepPropertiesV1 properties2;
  @Mock
  private Formats formats1;
  @Mock
  private Formats formats2;
  @Mock
  private TimestampColumn timestamp1;
  @Mock
  private TimestampColumn timestamp2;
  @Mock
  private LogicalSchema schema1;
  @Mock
  private LogicalSchema schema2;

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new TableSource(
                properties1, "topic1", formats1, Optional.of(timestamp1), schema1, 0, formats1),
            new TableSource(
                properties1, "topic1", formats1, Optional.of(timestamp1), schema1, 0, formats1),
            new TableSource(
                properties1, "topic1", formats1, Optional.of(timestamp1), schema1, 0, formats1))
        .addEqualityGroup(
            new TableSource(
                properties2, "topic1", formats1, Optional.of(timestamp1), schema1, 0, formats1))
        .addEqualityGroup(
            new TableSource(
                properties1, "topic2", formats1, Optional.of(timestamp1), schema1, 0, formats1))
        .addEqualityGroup(
            new TableSource(
                properties1, "topic1", formats2, Optional.of(timestamp1), schema1, 0, formats1))
        .addEqualityGroup(
            new TableSource(
                properties1, "topic1", formats1, Optional.of(timestamp2), schema1, 0, formats1))
        .addEqualityGroup(
            new TableSource(
                properties1, "topic1", formats1, Optional.of(timestamp1), schema2, 0, formats1))
        .addEqualityGroup(
            new TableSource(
                properties1, "topic1", formats1, Optional.of(timestamp1), schema1, 1, formats1))
        .addEqualityGroup(
            new TableSource(
                properties1, "topic1", formats1, Optional.of(timestamp1), schema2, 0, formats2))
        .testEquals();
  }
}