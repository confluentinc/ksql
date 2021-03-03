/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.rest.util.EntityUtil;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryStatus;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class QueryDescriptionFactory {

  private QueryDescriptionFactory() {
  }

  public static QueryDescription forQueryMetadata(
      final QueryMetadata queryMetadata,
      final Map<KsqlHostInfoEntity, KsqlQueryStatus> ksqlHostQueryStatus
  ) {
    if (queryMetadata instanceof PersistentQueryMetadata) {
      final PersistentQueryMetadata persistentQuery = (PersistentQueryMetadata) queryMetadata;
      return create(
          persistentQuery,
          persistentQuery.getResultTopic().getKeyFormat().getWindowType(),
          ImmutableSet.of(persistentQuery.getSinkName()),
          ksqlHostQueryStatus
      );
    }

    return create(
        queryMetadata,
        Optional.empty(),
        Collections.emptySet(),
        ksqlHostQueryStatus
    );
  }

  private static QueryDescription create(
      final QueryMetadata queryMetadata,
      final Optional<WindowType> windowType,
      final Set<SourceName> sinks,
      final Map<KsqlHostInfoEntity, KsqlQueryStatus> ksqlHostQueryStatus
  ) {
    return new QueryDescription(
        queryMetadata.getQueryId(),
        queryMetadata.getStatementString(),
        windowType,
        EntityUtil.buildSourceSchemaEntity(queryMetadata.getLogicalSchema()),
        queryMetadata.getSourceNames().stream().map(SourceName::text).collect(Collectors.toSet()),
        sinks.stream().map(SourceName::text).collect(Collectors.toSet()),
        queryMetadata.getTopologyDescription(),
        queryMetadata.getExecutionPlan(),
        queryMetadata.getOverriddenProperties(),
        ksqlHostQueryStatus,
        queryMetadata.getQueryType(),
        queryMetadata.getQueryErrors(),
        queryMetadata.getTaskMetadata()
    );
  }
}
