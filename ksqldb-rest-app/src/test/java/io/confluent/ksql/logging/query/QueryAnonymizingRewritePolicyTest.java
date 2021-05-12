package io.confluent.ksql.logging.query;

import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.engine.rewrite.QueryAnonymizer;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryGuid;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class QueryAnonymizingRewritePolicyTest {
  @Mock public KsqlConfig config;
  private QueryAnonymizingRewritePolicy policy;
  private QueryAnonymizer anonymizer = new QueryAnonymizer();

  private LoggingEvent buildLoggingEvent(Object msg) {
    final LoggingEvent loggingEvent = mock(LoggingEvent.class);
    when(loggingEvent.getLogger()).thenReturn(LogManager.getLogger("cattest"));
    when(loggingEvent.getMessage()).thenReturn(msg);
    return loggingEvent;
  }

  @Before
  public void setUp() throws Exception {
    // when
    when(config.getBoolean(KsqlConfig.KSQL_QUERYANONYMIZER_ENABLED)).thenReturn(true);
    when(config.getString(KsqlConfig.KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE))
        .thenReturn("cathouse.org.meowcluster");

    policy = new QueryAnonymizingRewritePolicy(config);
  }

  @Test
  public void shouldUseClusterNameAsNamespaceIfMissing() {
    when(config.getBoolean(KsqlConfig.KSQL_QUERYANONYMIZER_ENABLED)).thenReturn(true);
    when(config.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)).thenReturn("meowcluster");
    when(config.getString(KsqlConfig.KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE))
        .thenReturn("");

    policy = new QueryAnonymizingRewritePolicy(config);
    assertEquals("meowcluster", policy.getNamespace());

    when(config.getString(KsqlConfig.KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE)).thenReturn(null);
    policy = new QueryAnonymizingRewritePolicy(config);
    assertEquals("meowcluster", policy.getNamespace());
  }

  @Test
  public void shouldReplaceAQueryWithRewritten() {
    // when
    final LoggingEvent loggingEvent =
        buildLoggingEvent(
            new QueryLoggerMessage("cat", "CREATE TABLE CAT;"));
    // then
    final QueryLoggerMessage message =
        (QueryLoggerMessage) policy.rewrite(loggingEvent).getMessage();
    assertEquals(
        message.getQuery(),
        anonymizer.anonymize("CREATE TABLE CAT;"));
  }

  @Test
  public void shouldContainAQueryID() {
    // when
    final LoggingEvent loggingEvent =
        buildLoggingEvent(
            new QueryLoggerMessage("cat", "CREATE TABLE CAT;"));
    // then
    final QueryLoggerMessage message =
        (QueryLoggerMessage) policy.rewrite(loggingEvent).getMessage();
    final QueryGuid queryGuid = message.getQueryIdentifier();
    assertThat(queryGuid.getStructuralGuid(), not(isEmptyOrNullString()));
    assertThat(queryGuid.getQueryGuid(), not(isEmptyOrNullString()));
  }

  @Test
  public void shouldPassThroughIfAnonymizerDisabled() {
    //    given a config with disabled rewrite policy
    when(config.getBoolean(KsqlConfig.KSQL_QUERYANONYMIZER_ENABLED)).thenReturn(false);
    final QueryAnonymizingRewritePolicy rewritePolicy = new QueryAnonymizingRewritePolicy(config);
    //    when rewriting a LoggingEvent

    QueryLoggerMessage msg =
        new QueryLoggerMessage("cat", "CREATE TABLE CAT;");
    final LoggingEvent loggingEvent = buildLoggingEvent(msg);

    //    then we get nothing changed
    final QueryLoggerMessage result =
        (QueryLoggerMessage) rewritePolicy.rewrite(loggingEvent).getMessage();
    assertEquals(msg.getQuery(), result.getQuery());
    assertEquals(msg.getMessage(), result.getMessage());
    // and both guids are not the same
    assertNotEquals(
        result.getQueryIdentifier().getQueryGuid(), result.getQueryIdentifier().getStructuralGuid());
  }

  @Test
  public void shouldReturnLoggingEventUnchangedIfNotGivenAQueryLoggerMessage() {
    //    given a config with disabled rewrite policy
    when(config.getBoolean(KsqlConfig.KSQL_QUERYANONYMIZER_ENABLED)).thenReturn(false);
    final QueryAnonymizingRewritePolicy rewritePolicy = new QueryAnonymizingRewritePolicy(config);
    //    when rewriting a LoggingEvent

    String msg = "Cats are fast";
    final LoggingEvent loggingEvent = buildLoggingEvent(msg);

    //    then we get nothign changed
    final LoggingEvent result = rewritePolicy.rewrite(loggingEvent);
    assertEquals(loggingEvent, result);
  }
}
