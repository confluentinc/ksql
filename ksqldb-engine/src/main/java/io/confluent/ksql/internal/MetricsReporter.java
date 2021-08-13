/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.internal;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.common.Configurable;
import java.io.Closeable;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This interface is used to report metrics as data points to a
 * metrics framework.
 * The implementations of this interface specify how to report to
 * a specific metrics framework (e.g. JMX or Confluent's Telemetry
 * pipeline)
 */
public interface MetricsReporter extends Closeable, Configurable {

  /**
   * A data point that should be reported.
   */
  class DataPoint {
    private final String name;
    private final Instant time;
    private final Object value;
    private final ImmutableMap<String, String> tags;

    public DataPoint(final Instant time, final String name, final Object value) {
      this(time, name, value, Collections.emptyMap());
    }

    public DataPoint(
        final Instant time,
        final String name,
        final Object value,
        final Map<String, String> tags
    ) {
      this.name = Objects.requireNonNull(name, "name");
      this.time = Instant.from(Objects.requireNonNull(time, "time"));
      this.value = Objects.requireNonNull(value, "value");
      this.tags = ImmutableMap.copyOf(Objects.requireNonNull(tags, "tags"));
    }

    public String getName() {
      return name;
    }

    public Instant getTime() {
      return Instant.from(time);
    }

    public Object getValue() {
      return value;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "tags is ImmutableMap")
    public Map<String, String> getTags() {
      return tags;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final DataPoint dataPoint = (DataPoint) o;
      return time == dataPoint.time
          && Objects.equals(value, dataPoint.value)
          && Objects.equals(name, dataPoint.name)
          && Objects.equals(tags, dataPoint.tags);
    }

    @Override
    public String toString() {
      return "DataPoint{" + "name='" + name + '\''
          + ", time=" + time + ", value=" + value + ", tags=" + tags + '}';
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, time, value, tags);
    }
  }

  /**
   * Reports a list of data points.
   *
   * @param dataPoints the list of data points
   */
  void report(List<DataPoint> dataPoints);

  /**
   * Notifies the reporter that the metric with name and tags can be cleaned up
   */
  void cleanup(String name, Map<String, String> tags);
}
