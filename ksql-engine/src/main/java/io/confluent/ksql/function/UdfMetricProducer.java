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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;

import io.confluent.ksql.function.udf.Kudf;

/**
 * Capture metrics for a given Kudf
 */
class UdfMetricProducer implements Kudf {
  private final Sensor sensor;
  private final Kudf kudf;
  private final Time time;

  public UdfMetricProducer(final Sensor sensor,
                           final Kudf kudf,
                           final Time time) {
    this.sensor = sensor;
    this.kudf = kudf;
    this.time = time;
  }

  @Override
  public Object evaluate(final Object... args) {
    final long start = time.nanoseconds();
    try {
      return kudf.evaluate(args);
    } finally {
      sensor.record(time.nanoseconds() - start);
    }
  }
}
