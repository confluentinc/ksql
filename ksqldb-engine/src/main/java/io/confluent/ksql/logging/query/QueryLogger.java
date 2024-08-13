/*
 * Copyright 2021 Confluent Inc.
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 * http://www.confluent.io/confluent-community-license
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.logging.query;

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.engine.rewrite.QueryAnonymizer;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryGuid;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public final class QueryLogger {
  private static final Logger logger = LogManager.getLogger(QueryLogger.class);
  private static final QueryAnonymizer anonymizer = new QueryAnonymizer();

  private static String namespace = "";
  private static Boolean anonymizeQueries = true;

  private QueryLogger() {

  }

  @VisibleForTesting
  public static String getNamespace() {
    return namespace;
  }

  public static void addAppender(final Appender appender) {
    logger.addAppender(appender);
  }

  @SuppressFBWarnings(value = "MS_EXPOSE_REP")
  public static Logger getLogger() {
    return logger;
  }

  public static void configure(final KsqlConfig config) {
    final String clusterNamespace =
        config.getString(KsqlConfig.KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE);
    namespace =
        clusterNamespace == null || clusterNamespace.isEmpty()
            ? config.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)
            : clusterNamespace;
    anonymizeQueries = config.getBoolean(KsqlConfig.KSQL_QUERYANONYMIZER_ENABLED);
  }

  private static void log(final Level level, final Object message, final Statement query) {
    final String queryString = SqlFormatter.formatSql(query);
    log(level, message, queryString);
  }

  private static void log(final Level level, final Object message, final String query) {
    log(level, message, query, null);
  }

  private static void log(final Level level,
                          final Object message,
                          final String query,
                          final Throwable t) {
    try {
      final String anonQuery = anonymizeQueries
          ? anonymizer.anonymize(query) : query;
      final QueryGuid queryGuids = buildGuids(query, anonQuery);
      final QueryLoggerMessage payload = buildPayload(message, anonQuery, queryGuids);
      innerLog(level, payload, t);
    } catch (final Exception e) {
      final String unparsable = "<unparsable query>";
      final QueryLoggerMessage payload = buildPayload(
          message,
          unparsable,
          buildGuids(query, unparsable)
      );
      innerLog(level, payload, t);
    }
  }

  private static void innerLog(final Level level,
                               final QueryLoggerMessage payload,
                               final Throwable t) {
    if (t == null) {
      logger.log(level, payload);
    } else {
      logger.log(level, payload, t);
    }
  }

  private static QueryGuid buildGuids(final String query, final String anonymizedQuery) {
    return new QueryGuid(namespace, query, anonymizedQuery);
  }

  private static QueryLoggerMessage buildPayload(final Object message, final String query,
                                                 final QueryGuid guid) {
    return new QueryLoggerMessage(message, query, guid);
  }

  public static void debug(final Object message, final String query) {
    log(Level.DEBUG, message, query);
  }

  public static void debug(final Object message, final Statement query) {
    log(Level.DEBUG, message, query);
  }

  public static void info(final Object message, final String query) {
    log(Level.INFO, message, query);
  }

  public static void info(final Object message, final String query, final Throwable t) {
    log(Level.INFO, message, query, t);
  }

  public static void info(final Object message, final Statement query) {
    log(Level.INFO, message, query);
  }

  public static void warn(final Object message, final String query) {
    log(Level.WARN, message, query);
  }

  public static void warn(final Object message, final String query, final Throwable t) {
    log(Level.WARN, message, query, t);
  }

  public static void warn(final Object message, final Statement query) {
    log(Level.WARN, message, query);
  }

  public static void error(final Object message, final String query) {
    log(Level.ERROR, message, query);
  }

  public static void error(final Object message, final Statement query) {
    log(Level.ERROR, message, query);
  }

  public static void error(final String message, final String query, final Throwable t) {
    log(Level.ERROR, message, query, t);
  }
}
