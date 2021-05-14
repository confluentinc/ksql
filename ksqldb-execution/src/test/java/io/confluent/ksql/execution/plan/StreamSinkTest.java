/*
 * Copyright 2021 Confluent Inc.
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
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("UnstableApiUsage")
@RunWith(MockitoJUnitRunner.class)
public class StreamSinkTest {
  @Mock
  private ExecutionStepPropertiesV1 properties1;
  @Mock
  private ExecutionStepPropertiesV1 properties2;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> source1;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> source2;
  @Mock
  private Formats formats1;
  @Mock
  private Formats formats2;

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new StreamSink<>(properties1, source1, formats1, "topic1", Optional.empty()),
            new StreamSink<>(properties1, source1, formats1, "topic1", Optional.empty()))
        .addEqualityGroup(new StreamSink<>(properties2, source1, formats1, "topic1", Optional.empty()))
        .addEqualityGroup(new StreamSink<>(properties1, source2, formats1, "topic1", Optional.empty()))
        .addEqualityGroup(new StreamSink<>(properties1, source1, formats2, "topic1", Optional.empty()))
        .addEqualityGroup(new StreamSink<>(properties1, source1, formats1, "topic2", Optional.empty()))
        .addEqualityGroup(new StreamSink<>(properties1, source1, formats1, "topic1",
            Optional.of(new TimestampColumn(ColumnName.of("c1"), Optional.of("BIGINT")))))
        .testEquals();
  }
}