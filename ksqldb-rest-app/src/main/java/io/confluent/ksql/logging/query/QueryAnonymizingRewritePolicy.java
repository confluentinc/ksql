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

package io.confluent.ksql.logging.query;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.confluent.ksql.parser.ParsingException;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryAnonymizer;
import io.confluent.ksql.util.QueryGuid;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.log4j.Logger;
import org.apache.log4j.rewrite.RewritePolicy;
import org.apache.log4j.spi.LoggingEvent;

public final class QueryAnonymizingRewritePolicy implements RewritePolicy {
  private static final QueryAnonymizer anonymizer = new QueryAnonymizer();
  private final String namespace;
  private final Boolean anonymizeQueries;

  public QueryAnonymizingRewritePolicy(final KsqlConfig config) {
    final String clusterNamespace =
        config.getString(KsqlConfig.KSQL_CCLOUD_QUERYANONYMIZER_CLUSTER_NAMESPACE);
    this.namespace =
        clusterNamespace.isEmpty()
            ? config.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)
            : clusterNamespace;
    this.anonymizeQueries = config.getBoolean(KsqlConfig.KSQL_CCLOUD_QUERYANONYMIZER_ENABLED);
  }

  private QueryGuid buildGuids(final String query, final String anonymizedQuery) {
    return new QueryGuid(namespace, query, anonymizedQuery);
  }

  @Override
  public LoggingEvent rewrite(final LoggingEvent source) {
    final ImmutableMap<String, String> oldMessage =
        (ImmutableMap<String, String>) source.getMessage();
    final String query = oldMessage.get("query");

    final Map props = new HashMap(source.getProperties());

    String anonymizedQuery;
    try {
      anonymizedQuery = anonymizer.anonymize(query);
    } catch (ParsingException e) {
      anonymizedQuery = "";
    }

    final QueryGuid queryGuids = buildGuids(query, anonymizedQuery);
    final ImmutableMap<Object, Object> newMessage =
        ImmutableMap.builder()
            .put("query", anonymizeQueries ? anonymizedQuery : query)
            .put("queryGUID", queryGuids.getQueryGuid())
            .put("structuralGUID", queryGuids.getStructuralGuid())
            .build();

    final ImmutableMap<Object, Object> combined =
        Stream.concat(oldMessage.entrySet().stream(), newMessage.entrySet().stream())
            .collect(
                toImmutableMap(Map.Entry::getKey, Map.Entry::getValue, (left, right) -> right));

    return new LoggingEvent(
        source.getFQNOfLoggerClass(),
        source.getLogger() != null ? source.getLogger() : Logger.getLogger(source.getLoggerName()),
        source.getTimeStamp(),
        source.getLevel(),
        newMessageBuilder.build(),
        source.getThreadName(),
        source.getThrowableInformation(),
        source.getNDC(),
        source.getLocationInformation(),
        props);
  }
}
