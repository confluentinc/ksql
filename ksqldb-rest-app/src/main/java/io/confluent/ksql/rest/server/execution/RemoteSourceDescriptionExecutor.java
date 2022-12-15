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

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionList;
import java.util.List;

public final class RemoteSourceDescriptionExecutor {
  private RemoteSourceDescriptionExecutor() {
  }

  public static Multimap<String, SourceDescription> fetchSourceDescriptions(
      final RemoteHostExecutor remoteHostExecutor
  ) {
    final List<SourceDescription> sourceDescriptions = Maps
        .transformValues(
            remoteHostExecutor.fetchAllRemoteResults().getLeft(),
            SourceDescriptionList.class::cast)
        .values()
        .stream()
        .flatMap((rsl) -> rsl.getSourceDescriptions().stream())
        .collect(toImmutableList());

    return Multimaps.index(sourceDescriptions, SourceDescription::getName);

  }
}
