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

package io.confluent.ksql.metrics;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.common.utils.Time;
import io.confluent.ksql.metrics.TopicSensors.SensorMetric;
import io.confluent.ksql.metrics.TopicSensors.Stat;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Sensor;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class TopicSensorsTest {
  @Mock
  Sensor sensor;
  @Mock
  KafkaMetric metric;
  @Mock
  Time time;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Test
  public void shouldFormatTimestampInUnambiguousFormatAndUTC() {
    // Given:
    final Stat stat = new Stat("stat", 5.0, 1538842403035L);

    // When:
    final String timestamp = stat.timestamp();

    // Then:
    assertThat(timestamp, is("2018-10-06T16:13:23.035Z"));
  }

  @Test
  public void shouldGetMetricValueCorrectly() {
    // Given:
    final TopicSensors.SensorMetric sensorMetric = new SensorMetric(sensor, metric, time, false);

    // When:
    when(metric.metricValue()).thenReturn(1.2345);

    // Then:
    assertThat(sensorMetric.value(), equalTo(1.2345));
    verify(metric).metricValue();
  }
}