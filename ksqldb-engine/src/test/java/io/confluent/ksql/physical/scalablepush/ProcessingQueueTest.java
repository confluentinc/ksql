package io.confluent.ksql.physical.scalablepush;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.confluent.ksql.execution.streams.materialization.TableRow;
import io.confluent.ksql.query.QueryId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProcessingQueueTest {

  @Mock
  private Runnable runnable;
  @Mock
  private TableRow row1;
  @Mock
  private TableRow row2;
  @Mock
  private TableRow row3;

  @Test
  public void shouldOfferAndPoll() {
    // Given:
    final ProcessingQueue queue = new ProcessingQueue(new QueryId("a"));
    queue.setNewRowCallback(runnable);

    // When:
    assertThat(queue.offer(row1), is(true));
    assertThat(queue.offer(row2), is(true));
    assertThat(queue.offer(row3), is(true));

    // Then:
    verify(runnable, times(3)).run();
    assertThat(queue.poll(), is(row1));
    assertThat(queue.poll(), is(row2));
    assertThat(queue.poll(), is(row3));
    assertThat(queue.poll(), nullValue());
    assertThat(queue.hasDroppedRows(), is(false));
  }

  @Test
  public void shouldHitLimit() {
    // Given:
    final ProcessingQueue queue = new ProcessingQueue(new QueryId("a"), 2);
    queue.setNewRowCallback(runnable);

    // When:
    assertThat(queue.offer(row1), is(true));
    assertThat(queue.offer(row2), is(true));
    assertThat(queue.offer(row3), is(false));

    // Then:
    verify(runnable, times(2)).run();
    assertThat(queue.poll(), is(row1));
    assertThat(queue.poll(), is(row2));
    assertThat(queue.poll(), nullValue());
    assertThat(queue.hasDroppedRows(), is(true));
  }
}
