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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.engine.rewrite.QueryAnonymizer;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryGuid;
import org.apache.log4j.Logger;
import org.apache.log4j.rewrite.RewritePolicy;
import org.apache.log4j.spi.LoggingEvent;

public final class QueryAnonymizingRewritePolicy implements RewritePolicy {
  private static final QueryAnonymizer anonymizer = new QueryAnonymizer();

  @VisibleForTesting
  public String getNamespace() {
    return namespace;
  }

  @VisibleForTesting
  public Boolean getAnonymizeQueries() {
    return anonymizeQueries;
  }

  private final String namespace;
  private final Boolean anonymizeQueries;

  public QueryAnonymizingRewritePolicy(final KsqlConfig config) {
    final String clusterNamespace =
        config.getString(KsqlConfig.KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE);
    this.namespace =
        clusterNamespace == null || clusterNamespace.isEmpty()
            ? config.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)
            : clusterNamespace;
    this.anonymizeQueries = config.getBoolean(KsqlConfig.KSQL_QUERYANONYMIZER_ENABLED);
  }

  private QueryGuid buildGuids(final String query, final String anonymizedQuery) {
    return new QueryGuid(namespace, query, anonymizedQuery);
  }

  @Override
  public LoggingEvent rewrite(final LoggingEvent source) {
    final Object inputMessage = source.getMessage();
    if (!(inputMessage instanceof QueryLoggerMessage)) {
      return source;
    }
    final String query = ((QueryLoggerMessage) inputMessage).getQuery();
    final Object message = ((QueryLoggerMessage) inputMessage).getMessage();

    final String anonymizedQuery =
        anonymizeQueries ? anonymizer.anonymize(query) : query;

    final QueryGuid queryGuids = buildGuids(query, anonymizedQuery);
    final QueryLoggerMessage anonymized =
        new QueryLoggerMessage(message, anonymizedQuery, queryGuids);

    return new LoggingEvent(
        source.getFQNOfLoggerClass(),
        source.getLogger() != null ? source.getLogger() : Logger.getLogger(source.getLoggerName()),
        source.getTimeStamp(),
        source.getLevel(),
        anonymized,
        source.getThreadName(),
        source.getThrowableInformation(),
        source.getNDC(),
        source.getLocationInformation(),
        source.getProperties());
  }
}
