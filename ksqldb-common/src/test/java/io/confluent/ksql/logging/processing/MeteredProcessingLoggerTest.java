/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.logging.processing;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.confluent.ksql.logging.processing.ProcessingLogger.ErrorMessage;
import org.apache.kafka.common.metrics.Sensor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class MeteredProcessingLoggerTest {

  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private ErrorMessage errorMsg;
  @Mock
  private Sensor sensor;

  private ProcessingLogger meteredProcessingLogger;

  @Before
  public void setup() {
	meteredProcessingLogger = new MeteredProcessingLogger(processingLogger, sensor);
  }

  @Test
  public void shouldRecordMetricWhenError() {
	// When:
	meteredProcessingLogger.error(errorMsg);

	// Then:
	verify(processingLogger).error(errorMsg);
	verify(sensor).record();
  }

  @Test
  public void shouldRecordMetricWhenErrorFromDifferentLoggers() {
	// Given
	final ProcessingLogger otherMockProcessingLogger = mock(ProcessingLogger.class);
	final MeteredProcessingLogger differentLogger = new MeteredProcessingLogger(otherMockProcessingLogger, sensor);

	// When:
	differentLogger.error(errorMsg);
	meteredProcessingLogger.error(errorMsg);

	// Then:
	verify(otherMockProcessingLogger, times(1)).error(errorMsg);
	verify(processingLogger, times(1)).error(errorMsg);
	verify(sensor, times(2)).record();
  }
}