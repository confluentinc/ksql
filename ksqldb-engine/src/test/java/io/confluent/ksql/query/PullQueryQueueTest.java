package io.confluent.ksql.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.confluent.ksql.physical.pull.PullQueryRow;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PullQueryQueueTest {
  private static final int QUEUE_SIZE = 5;

  private static final PullQueryRow VAL_ONE = mock(PullQueryRow.class);
  private static final PullQueryRow VAL_TWO = mock(PullQueryRow.class);

  @Rule
  public final Timeout timeout = Timeout.seconds(10);

  @Mock
  private LimitHandler limitHandler;
  @Mock
  private Runnable queuedCallback;
  private PullQueryQueue queue;
  private ScheduledExecutorService executorService;

  @Before
  public void setUp() {
    givenQueue();
  }

  @After
  public void tearDown() {
    if (executorService != null) {
      executorService.shutdownNow();
    }
  }

  @Test
  public void shouldQueue() {
    // When:
    queue.acceptRow(VAL_ONE);
    queue.acceptRow(VAL_TWO);

    // Then:
    assertThat(drainValues(), contains(VAL_ONE, VAL_TWO));
    verify(queuedCallback, times(2)).run();
  }

  @Test
  public void shouldQueueUntilClosed() {
    // When:
    IntStream.range(0, QUEUE_SIZE)
        .forEach(idx -> {
          queue.acceptRow(VAL_ONE);
          if (idx == 2) {
            queue.close();
          }
        });

    // Then:
    assertThat(queue.size(), is(3));
    verify(queuedCallback, times(3)).run();
  }

  @Test
  public void shouldPoll() throws Exception {
    // Given:
    queue.acceptRow(VAL_ONE);
    queue.acceptRow(VAL_TWO);

    // When:
    final PullQueryRow result1 = queue.pollRow(1, TimeUnit.SECONDS);
    final PullQueryRow result2 = queue.pollRow(1, TimeUnit.SECONDS);
    final PullQueryRow result3 = queue.pollRow(1, TimeUnit.MICROSECONDS);

    // Then:
    assertThat(result1, is(VAL_ONE));
    assertThat(result2, is(VAL_TWO));
    assertThat(result3, is(nullValue()));
    verify(queuedCallback, times(2)).run();
  }

  @Test
  public void shouldCallLimitHandlerOnClose() {
    // When:
    queue.close();
    queue.close();

    // Then:
    verify(limitHandler, times(1)).limitReached();
  }


  @Test
  public void shouldBlockOnProduceOnceQueueLimitReachedAndUnblockOnClose() {
    // Given:
    givenQueue();

    IntStream.range(0, QUEUE_SIZE)
        .forEach(idx -> queue.acceptRow(VAL_ONE));

    givenWillCloseQueueAsync();

    // When:
    queue.acceptRow(VAL_TWO);

    // Then: did not block and:
    assertThat(queue.size(), is(QUEUE_SIZE));
    verify(queuedCallback, times(QUEUE_SIZE)).run();
  }

  private void givenWillCloseQueueAsync() {
    executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.schedule(queue::close, 200, TimeUnit.MILLISECONDS);
  }

  private void givenQueue() {
    queue = new PullQueryQueue(QUEUE_SIZE, 1);

    queue.setLimitHandler(limitHandler);
    queue.setQueuedCallback(queuedCallback);
  }

  private List<PullQueryRow> drainValues() {
    final List<PullQueryRow> entries = new ArrayList<>();
    queue.drainRowsTo(entries);
    return entries;
  }
}
