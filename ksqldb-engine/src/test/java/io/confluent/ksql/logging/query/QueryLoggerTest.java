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
import io.confluent.ksql.engine.rewrite.QueryAnonymizer;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryGuid;
import java.util.List;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class QueryLoggerTest {
  @Mock public KsqlConfig config;
  private final TestAppender testAppender = new TestAppender();

  private final QueryAnonymizer anonymizer = new QueryAnonymizer();

  @Before
  public void setUp() throws Exception {
    when(config.getBoolean(KsqlConfig.KSQL_QUERYANONYMIZER_ENABLED)).thenReturn(true);
    when(config.getString(KsqlConfig.KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE))
        .thenReturn("cathouse.org.meowcluster");
    testAppender.setName("TestAppender");
    final ConsoleAppender consoleAppender = new ConsoleAppender(new StructuredJsonLayout());
    consoleAppender.setName("console");
    QueryLogger.getLogger().addAppender(consoleAppender);

    QueryLogger.addAppender(testAppender);
    QueryLogger.configure(config);
  }

  @Test
  public void anonymizesQueriesForAllLogLevels() {
    String message = "my message";
    String query = "DESCRIBE cat EXTENDED;";
    String anonQuery = anonymizer.anonymize(query);

    QueryLogger.debug(message, query);
    QueryLogger.error(message, query);
    QueryLogger.info(message, query);
    QueryLogger.warn(message, query);
    testAppender
        .getLog()
        .forEach(
            (e) -> {
              final QueryLoggerMessage msg = (QueryLoggerMessage) e.getMessage();
              assertEquals(msg.getMessage(), message);
              assertEquals(msg.getQuery(), anonQuery);
            });
  }

  @Test
  public void shouldNotLogQueryIfQueryCannotBeParsed() {
    String message = " I love cats";
    String query = "CREATE CAT;";

    QueryLogger.debug(message, query);
    QueryLogger.error(message, query);
    QueryLogger.info(message, query);
    QueryLogger.warn(message, query);
    final List<LoggingEvent> events = testAppender.getLog();
    events
        .forEach(
            (e) -> {
              final QueryLoggerMessage msg = (QueryLoggerMessage) e.getMessage();
              assertEquals(msg.getMessage(), message);
              assertNotEquals(msg.getQuery(), query);
              assertEquals(msg.getQuery(), "<unparsable query>");
            });
  }

  @Test
  public void shouldLogQueryIfQueryCannotBeParsedIfAnonymizerIsDisabled() {
    when(config.getBoolean(KsqlConfig.KSQL_QUERYANONYMIZER_ENABLED)).thenReturn(false);
    QueryLogger.configure(config);

    String message = " I love cats";
    String query = "CREATE CAT;";

    QueryLogger.debug(message, query);
    QueryLogger.error(message, query);
    QueryLogger.info(message, query);
    QueryLogger.warn(message, query);
    final List<LoggingEvent> events = testAppender.getLog();
    events
        .forEach(
            (e) -> {
              final QueryLoggerMessage msg = (QueryLoggerMessage) e.getMessage();
              assertEquals(msg.getMessage(), message);
              assertEquals(msg.getQuery(), query);
            });
  }

  @Test
  public void shouldUseClusterNameAsNamespaceIfMissing() {
    // Given:
    when(config.getBoolean(KsqlConfig.KSQL_QUERYANONYMIZER_ENABLED)).thenReturn(true);
    when(config.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)).thenReturn("meowcluster");
    when(config.getString(KsqlConfig.KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE)).thenReturn("");
    QueryLogger.configure(config);
    assertEquals("meowcluster", QueryLogger.getNamespace());

    when(config.getString(KsqlConfig.KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE)).thenReturn(null);
    QueryLogger.configure(config);
    assertEquals("meowcluster", QueryLogger.getNamespace());
  }

  @Test
  public void shouldContainAQueryID() {
    String message = "my message";
    String query = "DESCRIBE cat EXTENDED;";

    QueryLogger.info(message, query);
    testAppender
        .getLog()
        .forEach(
            (e) -> {
              final QueryLoggerMessage msg = (QueryLoggerMessage) e.getMessage();
              final QueryGuid queryGuid = msg.getQueryIdentifier();
              assertThat(queryGuid.getStructuralGuid(), not(isEmptyOrNullString()));
              assertThat(queryGuid.getQueryGuid(), not(isEmptyOrNullString()));
            });
  }

  @Test
  public void shouldPassThroughIfAnonymizerDisabled() {
    // Given:
    when(config.getBoolean(KsqlConfig.KSQL_QUERYANONYMIZER_ENABLED)).thenReturn(false);
    QueryLogger.configure(config);

    // When:
    String message = "my message";
    String query = "DESCRIBE cat EXTENDED;";
    QueryLogger.info(message, query);

    // Then:
    testAppender
        .getLog()
        .forEach(
            (e) -> {
              final QueryLoggerMessage msg = (QueryLoggerMessage) e.getMessage();
              assertEquals(msg.getMessage(), message);
              assertEquals(msg.getQuery(), query);

              // both guids are not the same
              assertNotEquals(
                  msg.getQueryIdentifier().getQueryGuid(),
                  msg.getQueryIdentifier().getStructuralGuid());
            });
  }

  @Test
  public void shouldAnonymizeMultipleStatements() {
    QueryLogger.configure(config);
    QueryLogger.info("a message", "list streams; list tables; select a, b from mytable; list queries;");
    final List<LoggingEvent> events = testAppender.getLog();
    assertThat(events, hasSize(1));
    final LoggingEvent event = events.get(0);
    final QueryLoggerMessage message = (QueryLoggerMessage) event.getMessage();
    assertThat(message.getMessage(), is("a message"));
    assertThat(message.getQuery(), is("list STREAMS;\n" +
        "list TABLES;\n" +
        "SELECT column1, column2 FROM source1;\n" +
        "list QUERIES;"));
  }
}
