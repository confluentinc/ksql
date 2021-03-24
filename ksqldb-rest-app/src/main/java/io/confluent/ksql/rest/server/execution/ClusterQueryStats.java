/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.execution;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.metrics.TopicSensors;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.util.KsqlHostInfo;
import java.util.Collection;
import java.util.Map;

public final class ClusterQueryStats {
  private final ImmutableMap<KsqlHostInfo, ImmutableMap<String, TopicSensors.Stat>> stats;
  private final ImmutableMap<KsqlHostInfo, ImmutableMap<String, TopicSensors.Stat>> errors;
  private final String sourceName;

  private ClusterQueryStats(
      final ImmutableMap<KsqlHostInfo, ImmutableMap<String, TopicSensors.Stat>> stats,
      final ImmutableMap<KsqlHostInfo, ImmutableMap<String, TopicSensors.Stat>> errors,
      final String sourceName
  ) {
    this.stats = stats;
    this.errors = errors;
    this.sourceName = sourceName;
  }

  public static ClusterQueryStats create(
      final KsqlHostInfo localHostInfo,
      final SourceDescription localSourceDescription,
      final Collection<RemoteSourceDescription> remoteSourceDescriptions
  ) {
    final Map<KsqlHostInfo, SourceDescription> rds = remoteSourceDescriptions.stream()
        .collect(toImmutableMap(
            RemoteSourceDescription::getKsqlHostInfo,
            RemoteSourceDescription::getSourceDescription
        ));


    final ImmutableMap<KsqlHostInfo, ImmutableMap<String, TopicSensors.Stat>> remoteStats = rds
        .entrySet()
        .stream()
        .collect(
            toImmutableMap(
                Map.Entry::getKey,
                e -> ImmutableMap.copyOf(e.getValue().getStatisticsMap())
            )
        );
    final ImmutableMap<KsqlHostInfo, ImmutableMap<String, TopicSensors.Stat>> remoteErrors = rds
        .entrySet()
        .stream()
        .collect(
            toImmutableMap(
                Map.Entry::getKey,
                e -> ImmutableMap.copyOf(e.getValue().getErrorStatsMap())
            )
        );


    final ImmutableMap<KsqlHostInfo, ImmutableMap<String, TopicSensors.Stat>> stats = ImmutableMap
        .<KsqlHostInfo, ImmutableMap<String, TopicSensors.Stat>>builder()
        .put(localHostInfo, localSourceDescription.getStatisticsMap())
        .putAll(remoteStats)
        .build();

    final ImmutableMap<KsqlHostInfo, ImmutableMap<String, TopicSensors.Stat>> errors = ImmutableMap
        .<KsqlHostInfo, ImmutableMap<String, TopicSensors.Stat>>builder()
        .put(localHostInfo, localSourceDescription.getErrorStatsMap())
        .putAll(remoteErrors)
        .build();

    return new ClusterQueryStats(stats, errors, localSourceDescription.getName());
  }

  public ImmutableMap<KsqlHostInfo, ImmutableMap<String, TopicSensors.Stat>> getStats() {
    return stats;
  }

  public ImmutableMap<KsqlHostInfo, ImmutableMap<String, TopicSensors.Stat>> getErrors() {
    return errors;
  }

  public String getSourceName() {
    return sourceName;
  }
}
