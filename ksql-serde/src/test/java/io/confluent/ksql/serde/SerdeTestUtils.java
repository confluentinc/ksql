/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.serde;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import java.util.function.Function;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.mockito.ArgumentCaptor;

public final class SerdeTestUtils {
  private SerdeTestUtils() {
  }

  @SuppressWarnings("unchecked")
  public static void shouldLogError(
      final ProcessingLogger recordLogger,
      final SchemaAndValue expected,
      final ProcessingLogConfig config) {
    final ArgumentCaptor<Function> capture = ArgumentCaptor.forClass(Function.class);
    verify(recordLogger, times(1)).error(capture.capture());
    final Object errorMsg = capture.getValue().apply(config);
    assertThat(errorMsg, instanceOf(SchemaAndValue.class));
    assertThat(errorMsg, equalTo(expected));
  }
}
