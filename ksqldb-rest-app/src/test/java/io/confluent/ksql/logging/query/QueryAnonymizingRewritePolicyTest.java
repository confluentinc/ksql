package io.confluent.ksql.logging.query;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.engine.rewrite.QueryAnonymizer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.rewrite.RewriteAppender;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class QueryAnonymizingRewritePolicyTest {
  @Mock public KsqlConfig config;
  private Logger logger = LogManager.getLogger(QueryAnonymizingRewritePolicyTest.class);
  private TestAppender testAppender = new TestAppender();
  private QueryAnonymizer anonymizer = new QueryAnonymizer();

  @Before
  public void setUp() throws Exception {
    // when
    when(config.getBoolean(KsqlConfig.KSQL_QUERYANONYMIZER_ENABLED)).thenReturn(true);
    when(config.getString(KsqlConfig.KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE))
        .thenReturn("cathouse.org.meowcluster");

    final RewriteAppender rewriteAppender = new RewriteAppender();
    rewriteAppender.setRewritePolicy(new QueryAnonymizingRewritePolicy(config));
    rewriteAppender.addAppender(testAppender);
    LogManager.getLogger(QueryAnonymizingRewritePolicyTest.class).addAppender(rewriteAppender);
  }

  @Test
  public void shouldReplaceAQueryWithRewritten() {
    // when
    logger.error(ImmutableMap.of("message", "cat", "query", "CREATE TABLE CAT;"));
    // then
    final ImmutableMap<String, String> message =
        (ImmutableMap<String, String>) testAppender.getLog().get(0).getMessage();
    assertEquals(message.get("query"), anonymizer.anonymize("CREATE TABLE CAT;"));
  }

  @Test
  public void shouldContainAQueryID() {
    // when
    logger.error(ImmutableMap.of("message", "cat", "query", "CREATE TABLE CAT;"));
    // then
    final ImmutableMap<String, String> message =
        (ImmutableMap<String, String>) testAppender.getLog().get(0).getMessage();
    assertThat(message.get("structuralGUID"), not(isEmptyOrNullString()));
    assertThat(message.get("queryGUID"), not(isEmptyOrNullString()));
  }

  @Test
  public void shouldPassThroughIfAnonymizerDisabled() {
    //    given a config with disabled rewrite policy
    when(config.getBoolean(KsqlConfig.KSQL_QUERYANONYMIZER_ENABLED)).thenReturn(false);
    final QueryAnonymizingRewritePolicy rewritePolicy = new QueryAnonymizingRewritePolicy(config);
    //    when rewriting a LoggingEvent
    final LoggingEvent loggingEvent = mock(LoggingEvent.class);
    when(loggingEvent.getLogger()).thenReturn(LogManager.getLogger("cattest"));
    final ImmutableMap<String, String> msg =
        ImmutableMap.of("message", "cat", "query", "CREATE TABLE CAT;");
    when(loggingEvent.getMessage()).thenReturn(msg);
    //    then we get nothign changed
    final ImmutableMap<String, String> result =
        (ImmutableMap<String, String>) rewritePolicy.rewrite(loggingEvent).getMessage();
    assertEquals(msg.get("query"), result.get("query"));
  }
}
