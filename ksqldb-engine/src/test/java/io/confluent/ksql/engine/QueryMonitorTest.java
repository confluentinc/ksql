package io.confluent.ksql.engine;

import com.google.common.base.Ticker;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.PersistentQueryMetadata;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static org.apache.kafka.streams.KafkaStreams.State.ERROR;
import static org.apache.kafka.streams.KafkaStreams.State.RUNNING;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class QueryMonitorTest {
  private static long RETRY_BACKOFF_INITIAL_MS = 1;
  private static long RETRY_BACKOFF_MAX_MS = 10;
  private static long STATUS_RUNNING_THRESHOLD_MS = 15;

  @Mock
  private PersistentQueryMetadata queryMetadata;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private ExecutorService executor;
  @Mock
  private Ticker ticker;

  private QueryMonitor queryMonitor;

  @Before
  public void setup() {
    when(ticker.read()).thenReturn(1L);
    queryMonitor = new QueryMonitor(
        ksqlEngine,
        executor,
        RETRY_BACKOFF_INITIAL_MS,
        RETRY_BACKOFF_MAX_MS,
        STATUS_RUNNING_THRESHOLD_MS,
        ticker
    );
  }

  @Test
  public void shouldSubmitTaskOnStart() {
    // When:
    queryMonitor.start();

    // Then:
    final InOrder inOrder = inOrder(executor);
    inOrder.verify(executor).execute(any(Runnable.class));
    inOrder.verify(executor).shutdown();
    assertThat(queryMonitor.isClosed(), is(false));
  }

  @Test
  public void shouldCloseTheQueryMonitorCorrectly() throws Exception {
    // Given:
    queryMonitor.start();

    // When:
    queryMonitor.close();

    // Then:
    final InOrder inOrder = inOrder(executor);
    inOrder.verify(executor).awaitTermination(anyLong(), any());
    assertThat(queryMonitor.isClosed(), is(true));
  }

  @Test
  public void shouldRetryEventStartWithInitialValues() {
    // Given:
    final long now = 20;
    final QueryId queryId = new QueryId("id-1");
    when(ticker.read()).thenReturn(now);

    // When:
    final QueryMonitor.RetryEvent retryEvent = new QueryMonitor.RetryEvent(
        queryId,
        RETRY_BACKOFF_INITIAL_MS,
        RETRY_BACKOFF_MAX_MS,
        ticker
    );

    // Then:
    assertThat(retryEvent.getNumRetries(), is(0));
    assertThat(retryEvent.nextRestartTimeMs(), is(now + RETRY_BACKOFF_INITIAL_MS));
  }

  @Test
  public void shouldRetryEventRestartAndIncrementBackoffTime() {
    // Given:
    final long now = 20;
    final QueryId queryId = new QueryId("id-1");
    when(ticker.read()).thenReturn(now);

    // When:
    final QueryMonitor.RetryEvent retryEvent = new QueryMonitor.RetryEvent(
        queryId,
        RETRY_BACKOFF_INITIAL_MS,
        RETRY_BACKOFF_MAX_MS,
        ticker
    );
    retryEvent.restart(queryMetadata);

    // Then:
    assertThat(retryEvent.getNumRetries(), is(1));
    assertThat(retryEvent.nextRestartTimeMs(), is(now + RETRY_BACKOFF_INITIAL_MS * 2));
    verify(queryMetadata).restart();
  }

  @Test
  public void shouldRetryEventRestartAndNotExceedBackoffMaxTime() {
    // Given:
    final long now = 20;
    final QueryId queryId = new QueryId("id-1");
    when(ticker.read()).thenReturn(now);

    // When:
    final QueryMonitor.RetryEvent retryEvent = new QueryMonitor.RetryEvent(
        queryId,
        RETRY_BACKOFF_INITIAL_MS,
        RETRY_BACKOFF_MAX_MS,
        ticker
    );
    retryEvent.restart(queryMetadata);
    retryEvent.restart(queryMetadata);

    // Then:
    assertThat(retryEvent.getNumRetries(), is(2));
    assertThat(retryEvent.nextRestartTimeMs(), lessThan(now + RETRY_BACKOFF_MAX_MS));
  }

  @Test
  public void shouldRetryEventNotThrowIfRestartThrowsException() {
    // Given:
    final long now = 20;
    final QueryId queryId = new QueryId("id-1");
    when(ticker.read()).thenReturn(now);
    doThrow(IllegalStateException.class).when(queryMetadata).restart();

    // When:
    final QueryMonitor.RetryEvent retryEvent = new QueryMonitor.RetryEvent(
        queryId,
        RETRY_BACKOFF_INITIAL_MS,
        RETRY_BACKOFF_MAX_MS,

        ticker
    );
    retryEvent.restart(queryMetadata);

    // Then:
    verify(queryMetadata).restart();
  }

  @Test
  public void shouldNotRestartRunningQueries() {
    // Given:
    final PersistentQueryMetadata query1 = mockPersistentQueryMetadata("id-1", RUNNING);
    final PersistentQueryMetadata query2 = mockPersistentQueryMetadata("id-2", RUNNING);
    when(ksqlEngine.getPersistentQueries()).thenReturn(Arrays.asList(query1, query2));

    // When:
    queryMonitor.restartFailedQueries();

    // Then:
    verify(query1, never()).stop();
    verify(query1, never()).start();
    verify(query2, never()).stop();
    verify(query2, never()).start();
  }

  @Test
  public void shouldNoRestartQueryThatWasManuallyTerminated() {
    // Given:
    final PersistentQueryMetadata query = mockPersistentQueryMetadata("id-1", ERROR);
    when(ksqlEngine.getPersistentQueries()).thenReturn(Arrays.asList(query));
    when(ksqlEngine.getPersistentQuery(query.getQueryId())).thenReturn(Optional.of(query));

    // When
    when(ticker.read()).thenReturn(1L).thenReturn(RETRY_BACKOFF_INITIAL_MS + 2);
    queryMonitor.restartFailedQueries(); // 1st restart

    // Mock the query is in ERROR state, but then terminated manually
    when(query.isError()).thenReturn(true);
    when(ksqlEngine.getPersistentQuery(query.getQueryId())).thenReturn(Optional.empty());

    // Internally, the query is found as ERROR because getPersistentQueries() returns it, but
    // it will never be restarted because getPersistentQuery() does not return it.
    queryMonitor.restartFailedQueries(); // 2nd restart will not restart a non-present query

    // Then:
    verify(query, times(1)).restart();
  }

  @Test
  public void shouldRestartNewQueryInErrorState() {
    // Given:
    final PersistentQueryMetadata query = mockPersistentQueryMetadata("id-1", ERROR);
    when(ksqlEngine.getPersistentQueries()).thenReturn(Arrays.asList(query));
    when(ksqlEngine.getPersistentQuery(query.getQueryId())).thenReturn(Optional.of(query));

    // When:
    when(ticker.read()).thenReturn(1L).thenReturn(RETRY_BACKOFF_INITIAL_MS + 2);
    queryMonitor.restartFailedQueries();

    // Then:
    verify(query).restart();
  }

  @Test
  public void shouldRestartQueryInErrorStateAgainAfterBackoffTime() {
    // Given:
    final PersistentQueryMetadata query = mockPersistentQueryMetadata("id-1", ERROR);
    when(ksqlEngine.getPersistentQueries()).thenReturn(Arrays.asList(query));
    when(ksqlEngine.getPersistentQuery(query.getQueryId())).thenReturn(Optional.of(query));

    // When:
    when(ticker.read()).thenReturn(1L).thenReturn(RETRY_BACKOFF_INITIAL_MS + 2);
    queryMonitor.restartFailedQueries();
    when(ticker.read()).thenReturn(RETRY_BACKOFF_INITIAL_MS * 2 + 3);
    queryMonitor.restartFailedQueries();

    // Then:
    verify(query, times(2)).restart();
  }

  @Test
  public void shouldNotRestartQueryInErrorStateBeforeBackoffTime() {
    // Given:
    final PersistentQueryMetadata query = mockPersistentQueryMetadata("id-1", ERROR);
    when(ksqlEngine.getPersistentQueries()).thenReturn(Arrays.asList(query));
    when(ksqlEngine.getPersistentQuery(query.getQueryId())).thenReturn(Optional.of(query));
    when(ticker.read()).thenReturn(RETRY_BACKOFF_INITIAL_MS - 1);

    // When:
    queryMonitor.restartFailedQueries();
    queryMonitor.restartFailedQueries();

    // Then:
    verify(query, never()).restart();
  }

  @Test
  public void shouldNotRestartQueryInErrorThenRunningAfterBackoffTime() {
    // Given:
    final PersistentQueryMetadata query = mockPersistentQueryMetadata("id-1", ERROR);
    when(ksqlEngine.getPersistentQueries()).thenReturn(Arrays.asList(query));
    when(ksqlEngine.getPersistentQuery(query.getQueryId())).thenReturn(Optional.of(query));

    // When:
    when(ticker.read()).thenReturn(1L).thenReturn(RETRY_BACKOFF_INITIAL_MS + 2);
    queryMonitor.restartFailedQueries();
    when(ticker.read()).thenReturn(RETRY_BACKOFF_INITIAL_MS + 3);
    when(query.isError()).thenReturn(false);
    when(query.getState()).thenReturn(RUNNING);
    queryMonitor.restartFailedQueries(); // 2nd round should not restart before backoff time

    // Then:
    verify(query, times(1)).restart();
  }

  @Test
  public void shouldRestartedQueryNotClearErrorsBeforeHealthyTime() {
    // Given:
    final PersistentQueryMetadata query = mockPersistentQueryMetadata("id-1", ERROR);
    when(ksqlEngine.getPersistentQueries()).thenReturn(Arrays.asList(query));
    when(ksqlEngine.getPersistentQuery(query.getQueryId())).thenReturn(Optional.of(query));

    // When:
    queryMonitor.restartFailedQueries();
    when(ticker.read()).thenReturn(RETRY_BACKOFF_INITIAL_MS + 1);
    when(query.isError()).thenReturn(false);
    when(query.getState()).thenReturn(RUNNING);
    // 2nd round should not clear query errors before healthy time
    queryMonitor.restartFailedQueries();

    // Then:
    verify(query, never()).clearErrors();
  }

  @Test
  public void shouldRestartedQueryClearErrorsAfterHealthyTime() {
    // Given:
    final PersistentQueryMetadata query = mockPersistentQueryMetadata("id-1", ERROR);
    when(ksqlEngine.getPersistentQueries()).thenReturn(Arrays.asList(query));
    when(ksqlEngine.getPersistentQuery(query.getQueryId())).thenReturn(Optional.of(query));

    // When:
    queryMonitor.restartFailedQueries();
    when(query.isError()).thenReturn(false);
    when(query.getState()).thenReturn(RUNNING);
    when(query.uptime()).thenReturn(STATUS_RUNNING_THRESHOLD_MS + 1);
    // 2nd round should not clear query errors before healthy time
    queryMonitor.restartFailedQueries();

    // Then:
    verify(query, times(1)).clearErrors();
  }

  private PersistentQueryMetadata mockPersistentQueryMetadata(
      final String queryId,
      final KafkaStreams.State queryState
  ) {
    final PersistentQueryMetadata query = mock(PersistentQueryMetadata.class);
    when(query.getQueryId()).thenReturn(new QueryId(queryId));
    when(query.isError()).thenReturn(queryState == ERROR);
    when(query.getState()).thenReturn(ERROR);
    return query;
  }
}
