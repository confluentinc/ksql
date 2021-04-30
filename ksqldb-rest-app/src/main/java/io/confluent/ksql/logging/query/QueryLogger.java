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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.log4j.Appender;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.rewrite.RewriteAppender;

public final class QueryLogger {
  private static final Logger logger = LogManager.getLogger(QueryLogger.class);
  private static final RewriteAppender rewriteAppender = new RewriteAppender();

  private QueryLogger() {}

  public static void initialize() {
    logger.setAdditivity(false);

    final Enumeration enumeration = logger.getAllAppenders();
    final ArrayList<Appender> list = Collections.list(enumeration);
    logger.addAppender(rewriteAppender);
    for (Appender a : list) {
      if (a != rewriteAppender) {
        logger.removeAppender(a);
        rewriteAppender.addAppender(a);
      }
    }
  }

  public static void addAppender(final Appender appender) {
    logger.addAppender(appender);
  }

  public static Logger getLogger() {
    return logger;
  }

  public static void configure(final KsqlConfig config) {
    rewriteAppender.setRewritePolicy(new QueryAnonymizingRewritePolicy(config));
  }

  private static ImmutableMap<String, Object> buildPayload(
      final Object message, final String query) {
    return ImmutableMap.of("message", message, "query", query);
  }

  private static ImmutableMap<String, Object> buildPayload(
      final Object message, final ParseTree query) {
    return ImmutableMap.of("message", message, "query", query);
  }

  public static void debug(final Object message) {
    logger.debug(message);
  }

  public static void debug(final Object message, final String query) {
    logger.debug(buildPayload(message, query));
  }

  public static void debug(final Object message, final ParseTree query) {
    logger.debug(buildPayload(message, query));
  }

  public static void info(final Object message) {
    logger.info(message);
  }

  public static void info(final Object message, final String query) {
    logger.info(buildPayload(message, query));
  }

  public static void info(final Object message, final ParseTree query) {
    logger.info(buildPayload(message, query));
  }

  public static void warn(final Object message) {
    logger.warn(message);
  }

  public static void warn(final Object message, final String query) {
    logger.warn(buildPayload(message, query));
  }

  public static void warn(final Object message, final ParseTree query) {
    logger.warn(buildPayload(message, query));
  }

  public static void error(final Object message) {
    logger.error(message);
  }

  public static void error(final Object message, final String query) {
    logger.error(buildPayload(message, query));
  }

  public static void error(final Object message, final ParseTree query) {
    logger.error(buildPayload(message, query));
  }
}
