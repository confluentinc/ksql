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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MeteredProcessingLoggerTest {
  private final static String sensorName = "sensorName";
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private ProcessingLogger.ErrorMessage errMessage;
  @Mock
  private Sensor sensor;
  @Mock
  private Metrics metrics;

  private MeteredProcessingLogger meteredProcessingLogger;

  @Before
  public void setup() {
	when(sensor.name()).thenReturn(sensorName);
	meteredProcessingLogger = new MeteredProcessingLogger(processingLogger, metrics, sensor);
  }

  @Test
  public void shouldRecordMetric() {
	// When:
	meteredProcessingLogger.error(errMessage);
	meteredProcessingLogger.error(errMessage);

	// Then:
	verify(processingLogger, times(2)).error(errMessage);
	verify(sensor, times(2)).record();
  }

  @Test
  public void shouldRemoveSensorOnClose() {
	// When:
	meteredProcessingLogger.close();

	// Then:
	verify(metrics).removeSensor(sensorName);
	verify(processingLogger).close();
  }

  @Test
  public void shouldHandleNullMetricsAndSensor() {
	// Given:
	meteredProcessingLogger = new MeteredProcessingLogger(processingLogger, null, null);

	// When:
	meteredProcessingLogger.error(errMessage);
	meteredProcessingLogger.close();

	// Then:
	verify(processingLogger, times(1)).error(errMessage);
	verify(sensor, never()).record();
	verify(metrics, never()).removeSensor(sensorName);
	verify(processingLogger).close();
  }
}