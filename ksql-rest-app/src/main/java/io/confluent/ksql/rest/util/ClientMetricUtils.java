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

package io.confluent.ksql.rest.util;

import com.google.common.collect.Iterables;
import com.google.common.math.DoubleMath;
import io.confluent.ksql.rest.entity.Metric;
import io.confluent.ksql.rest.entity.Metrics;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/**
 * Contains <i>client</i> side utility methods for handling {@link Metric}
 * and {@link Metrics}.
 */
public final class ClientMetricUtils {

  private static final String TABLE_HEADER = String.format(
      "|%s|%s|%s|%s|",
      StringUtils.center("metric", 40),
      StringUtils.center("value", 14),
      StringUtils.center("timestamp", 14),
      StringUtils.center("error", 7));
  private static final String TABLE_CONTENT_FORMAT = "|%-40s|%14.6e|%14d|%7s|";
  private static final String TABLE_ROW = "|" + StringUtils.repeat('-', 78) + "|";

  private ClientMetricUtils() { }

  public static String format(final Metrics metrics) {
    final StringBuilder table = new StringBuilder();

    table.append(TABLE_ROW);
    table.append(System.lineSeparator());
    table.append(TABLE_HEADER);
    table.append(System.lineSeparator());
    table.append(TABLE_ROW);
    table.append(System.lineSeparator());

    for (final Metric metric : Iterables.concat(metrics.getMetrics(), metrics.getErrorMetrics())) {
      table.append(String.format(
          TABLE_CONTENT_FORMAT,
          metric.getName(),
          metric.getValue(),
          metric.getTimestamp(),
          StringUtils.center(metric.isError() ? "x" : "", 7)));
      table.append(System.lineSeparator());
    }

    if (metrics.getMetrics().isEmpty() && metrics.getErrorMetrics().isEmpty()) {
      table.append("|");
      table.append(StringUtils.center("No Metrics Available", 78));
      table.append("|");
      table.append(System.lineSeparator());
    }

    table.append(TABLE_ROW);
    return table.toString();
  }

  public static String format(final Collection<Metric> metrics, final boolean errors) {
    final String lastEventTimestampMessage;
    if (!metrics.isEmpty()) {
      lastEventTimestampMessage =
          String.format(" %16s: ", errors ? "last-failed" : "last-message")
              + String.format("%9s", metrics.iterator().next());
    } else {
      lastEventTimestampMessage = "";
    }

    return metrics.stream().map(ClientMetricUtils::format).collect(Collectors.joining(" "))
        + lastEventTimestampMessage ;
  }

  private static String format(Metric metric) {
    return (DoubleMath.isMathematicalInteger(metric.getValue()))
        ? String.format("%16s:%10.0f", metric.getName(), metric.getValue())
        : String.format("%16s:%10.2f", metric.getName(), metric.getValue());
  }

}
