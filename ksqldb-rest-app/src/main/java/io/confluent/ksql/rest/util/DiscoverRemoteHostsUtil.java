/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.util;

import io.confluent.ksql.util.KsqlHostInfo;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.HostInfo;

public final class DiscoverRemoteHostsUtil {

  private DiscoverRemoteHostsUtil() {

  }

  public static Set<HostInfo> getRemoteHosts(
      final List<PersistentQueryMetadata> currentQueries, 
      final KsqlHostInfo localHost
  ) {
    return currentQueries.stream()
        // required filter else QueryMetadata.getAllMetadata() throws
        .filter(q -> q.getState().isRunningOrRebalancing())
        .map(QueryMetadata::getAllStreamsHostMetadata)
        .filter(Objects::nonNull)
        .flatMap(Collection::stream)
        .map(StreamsMetadata::hostInfo)
        .filter(hostInfo -> !(hostInfo.host().equals(localHost.host())
            && hostInfo.port() == (localHost.port())))
        .collect(Collectors.toSet());
  }
}
