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

import static com.google.common.collect.ImmutableListMultimap.flatteningToImmutableListMultimap;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import io.confluent.ksql.rest.entity.SourceDescriptionList;
import io.confluent.ksql.util.KsqlHostInfo;
import java.util.Map;

public final class RemoteSourceDescriptionExecutor {
  private RemoteSourceDescriptionExecutor() {
  }

  public static Multimap<String, RemoteSourceDescription> fetchSourceDescriptions(
      final RemoteHostExecutor remoteHostExecutor
  ) {
    return Maps
        .transformValues(
            remoteHostExecutor.fetchAllRemoteResults().getLeft(),
            SourceDescriptionList.class::cast)
        .entrySet()
        .stream()
        .collect(
            flatteningToImmutableListMultimap(
                Map.Entry::getKey,
                (e) -> e.getValue().getSourceDescriptions().stream())
        )
        .entries()
        .stream()
        .collect(toImmutableListMultimap(
            e -> e.getValue().getName(),
            e -> new RemoteSourceDescription(
                e.getValue().getName(),
                e.getValue(),
                KsqlHostInfo.fromHostInfo(e.getKey())
            )
        ));
  }
}
