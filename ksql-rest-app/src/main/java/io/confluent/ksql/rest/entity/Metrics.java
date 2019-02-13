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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("metrics")
public class Metrics {

  private static final String TABLE_HEADER = String.format(
      "|%s|%s|%s|%s|",
      StringUtils.center("metric", 40),
      StringUtils.center("value", 14),
      StringUtils.center("timestamp", 14),
      StringUtils.center("error", 7));
  private static final String TABLE_CONTENT_FORMAT = "|%-40s|%14.6e|%14d|%7s|";
  private static final String TABLE_ROW = "|" + StringUtils.repeat('-', 78) + "|";

  private final Collection<Metric> metrics;
  private final Collection<Metric> errorMetrics;

  @JsonCreator
  public Metrics(
      @JsonProperty("metrics") final Collection<Metric> metrics,
      @JsonProperty("errorMetrics") final Collection<Metric> errorMetrics
  ) {
    this.metrics = metrics;
    this.errorMetrics = errorMetrics;
  }

  public Collection<Metric> getMetrics() {
    return metrics;
  }

  public Collection<Metric> getErrorMetrics() {
    return errorMetrics;
  }

  String formatted(final boolean errors) {
    final Collection<Metric> metrics = errors ? this.errorMetrics : this.metrics;

    final String lastEventTimestampMessage;
    if (!metrics.isEmpty()) {
      lastEventTimestampMessage =
          String.format(" %16s: ", errors ? "last-failed" : "last-message")
              + String.format("%9s", metrics.iterator().next());
    } else {
      lastEventTimestampMessage = "";
    }

    return metrics.stream().map(Metric::formatted).collect(Collectors.joining(" "))
        + lastEventTimestampMessage ;
  }

  public String format() {
    final StringBuilder table = new StringBuilder();

    table.append(TABLE_ROW);
    table.append(System.lineSeparator());
    table.append(TABLE_HEADER);
    table.append(System.lineSeparator());
    table.append(TABLE_ROW);
    table.append(System.lineSeparator());

    for (final Metric metric : Iterables.concat(metrics, errorMetrics)) {
      table.append(String.format(
          TABLE_CONTENT_FORMAT,
          metric.getName(),
          metric.getValue(),
          metric.getTimestamp(),
          StringUtils.center(metric.isError() ? "x" : "", 7)));
      table.append(System.lineSeparator());
    }

    if (metrics.isEmpty() && errorMetrics.isEmpty()) {
      table.append("|");
      table.append(StringUtils.center("No Metrics Available", 78));
      table.append("|");
      table.append(System.lineSeparator());
    }

    table.append(TABLE_ROW);
    return table.toString();
  }

  @Override
  public String toString() {
    return format();
  }
}
