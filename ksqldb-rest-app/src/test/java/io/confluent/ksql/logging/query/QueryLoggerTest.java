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

import io.confluent.common.logging.log4j.StructuredJsonLayout;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.rewrite.RewriteAppender;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class QueryLoggerTest {
  @Mock public KsqlConfig config;
  private TestAppender testAppender = new TestAppender();

  @Before
  public void setUp() throws Exception {
    // when
    when(config.getBoolean(KsqlConfig.KSQL_QUERYANONYMIZER_ENABLED)).thenReturn(true);
    when(config.getString(KsqlConfig.KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE))
        .thenReturn("cathouse.org.meowcluster");
    testAppender.setName("TestAppender");
    final ConsoleAppender consoleAppender = new ConsoleAppender(new StructuredJsonLayout());
    consoleAppender.setName("console");
    QueryLogger.getLogger().addAppender(consoleAppender);

    QueryLogger.addAppender(testAppender);
    QueryLogger.initialize();
    QueryLogger.configure(config);
  }

  @Test
  public void shouldConfigureNonAdditiveLogger() {
    assertFalse(QueryLogger.getLogger().getAdditivity());
  }

  @Test
  public void shouldAddRewriterBeforeExistingAppenders() {
    // Given a QueryLogger configured elsewhere with a test appender
    // then I have a log rewrite appender
    final RewriteAppender rewriteAppender =
        (RewriteAppender) QueryLogger.getLogger().getAllAppenders().nextElement();
    assertThat(rewriteAppender, instanceOf(RewriteAppender.class));
    // then I have my configured appender added to rewrite appender
    assertNotNull(rewriteAppender.getAppender(testAppender.getName()));
  }

  @Test
  public void createsPayloadsForAllLogLevels() {
    // Given a logger
    // when we log pairs message/query
    String message = "my message";
    String query = "create table cat;";
    QueryLogger.debug(message, query);
    QueryLogger.error(message, query);
    QueryLogger.info(message, query);
    QueryLogger.warn(message, query);
    // they end up rewritten
    testAppender
        .getLog()
        .forEach(
            (e) -> {
              final QueryLoggerMessage msg = (QueryLoggerMessage) e.getMessage();
              assertEquals(msg.getMessage(), message);
              assertNotEquals(msg.getQuery(), query);
            });
  }

  @Test
  public void shouldNotLogIfQueryCannotBeParsed() {
    String message = " I love cats";
    String query = "CREATE CAT;";

    QueryLogger.debug(message, query);
    QueryLogger.error(message, query);
    QueryLogger.info(message, query);
    QueryLogger.warn(message, query);
    testAppender
        .getLog()
        .forEach(
            (e) -> {
              final QueryLoggerMessage msg = (QueryLoggerMessage) e.getMessage();
              assertNotEquals(msg.getMessage(), message);
              assertNotEquals(msg.getQuery(), query);
            });
  }
}
