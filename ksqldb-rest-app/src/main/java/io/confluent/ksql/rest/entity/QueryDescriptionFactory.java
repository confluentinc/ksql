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
import io.confluent.ksql.util.PersistentQueryEntity;
import io.confluent.ksql.util.QueryEntity;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class QueryDescriptionFactory {

  private QueryDescriptionFactory() {
  }

  public static QueryDescription forQueryMetadata(
      final QueryEntity queryEntity,
      final Map<KsqlHostInfoEntity, KsqlQueryStatus> ksqlHostQueryStatus
  ) {
    if (queryEntity instanceof PersistentQueryEntity) {
      final PersistentQueryEntity persistentQuery = (PersistentQueryEntity) queryEntity;
      return create(
          persistentQuery,
          persistentQuery.getResultTopic().getKeyFormat().getWindowType(),
          ImmutableSet.of(persistentQuery.getSinkName()),
          ksqlHostQueryStatus
      );
    }

    return create(
            queryEntity,
        Optional.empty(),
        Collections.emptySet(),
        ksqlHostQueryStatus
    );
  }

  private static QueryDescription create(
      final QueryEntity queryEntity,
      final Optional<WindowType> windowType,
      final Set<SourceName> sinks,
      final Map<KsqlHostInfoEntity, KsqlQueryStatus> ksqlHostQueryStatus
  ) {
    return new QueryDescription(
        queryEntity.getQueryId(),
        queryEntity.getStatementString(),
        windowType,
        EntityUtil.buildSourceSchemaEntity(queryEntity.getLogicalSchema()),
        queryEntity.getSourceNames().stream().map(SourceName::text).collect(Collectors.toSet()),
        sinks.stream().map(SourceName::text).collect(Collectors.toSet()),
        queryEntity.getTopologyDescription(),
        queryEntity.getExecutionPlan(),
        queryEntity.getOverriddenProperties(),
        ksqlHostQueryStatus,
        queryEntity.getQueryType(),
        queryEntity.getQueryErrors(),
        queryEntity.getTaskMetadata()
    );
  }
}
