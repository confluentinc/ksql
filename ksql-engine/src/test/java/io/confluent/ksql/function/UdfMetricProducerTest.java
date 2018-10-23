/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Before;
import org.junit.Test;

public class UdfMetricProducerTest {

  private final Time time = new MockTime();
  private final Metrics metrics = new Metrics(time);
  private final Sensor sensor = metrics.sensor("my-udf");
  private final MetricName metricName = metrics.metricName("avg", "blah");

  @Before
  public void before() {
    sensor.add(metricName, new Avg());
  }

  @Test
  public void shouldRecordMetrics() {
    final UdfMetricProducer metricProducer
        = new UdfMetricProducer(sensor, args -> {
      time.sleep(100);
      return null;
    }, time);

    metricProducer.evaluate("foo");

    final KafkaMetric metric = metrics.metric(metricName);
    final Double actual = (Double) metric.metricValue();
    assertThat(actual.longValue(), equalTo(TimeUnit.MILLISECONDS.toNanos(100)));
  }

  @Test
  public void shouldRecordEvenIfExceptionThrown(){
    final UdfMetricProducer metricProducer
        = new UdfMetricProducer(sensor, args -> {
          time.sleep(10);
     throw new RuntimeException("boom");
    }, time);

    try {
      metricProducer.evaluate("foo");
    } catch (final Exception e) {
      // ignored
    }

    final KafkaMetric metric = metrics.metric(metricName);
    final Double actual = (Double) metric.metricValue();
    assertThat(actual.longValue(), equalTo(TimeUnit.MILLISECONDS.toNanos(10)));
  }
}