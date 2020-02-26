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
import io.confluent.ksql.name.ColumnName;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Optional;

@RunWith(MockitoJUnitRunner.class)
public class TableSinkTest {
  @Mock
  private ExecutionStepPropertiesV1 properties1;
  @Mock
  private ExecutionStepPropertiesV1 properties2;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> source1;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> source2;
  @Mock
  private Formats formats1;
  @Mock
  private Formats formats2;

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new TableSink<>(properties1, source1, formats1, "topic1", Optional.empty()),
            new TableSink<>(properties1, source1, formats1, "topic1", Optional.empty()))
        .addEqualityGroup(new TableSink<>(properties2, source1, formats1, "topic1", Optional.empty()))
        .addEqualityGroup(new TableSink<>(properties1, source2, formats1, "topic1", Optional.empty()))
        .addEqualityGroup(new TableSink<>(properties1, source1, formats2, "topic1", Optional.empty()))
        .addEqualityGroup(new TableSink<>(properties1, source1, formats1, "topic2", Optional.empty()))
        .addEqualityGroup(new TableSink<>(properties1, source1, formats1, "topic1",
            Optional.of(new TimestampColumn(ColumnName.of("c1"), Optional.of("BIGINT")))));;
  }
}