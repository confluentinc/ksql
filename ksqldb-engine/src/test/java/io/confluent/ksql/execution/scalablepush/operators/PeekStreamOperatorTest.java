package io.confluent.ksql.execution.scalablepush.operators;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.confluent.ksql.execution.common.QueryRow;
import io.confluent.ksql.execution.scalablepush.ProcessingQueue;
import io.confluent.ksql.execution.scalablepush.ScalablePushRegistry;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.query.QueryId;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PeekStreamOperatorTest {

  private static final QueryId QUERY_ID = new QueryId("foo");

  @Mock
  private ScalablePushRegistry registry;
  @Mock
  private DataSourceNode dataSourceNode;
  @Captor
  private ArgumentCaptor<ProcessingQueue> processingQueueCaptor;
  @Mock
  private QueryRow row1;
  @Mock
  private QueryRow row2;
  @Mock
  private Runnable newRowCallback;
  @Mock
  private ProcessingQueue processingQueue;

  @Test
  public void shouldGetRowsFromOperator() {
    // Given:
    final PeekStreamOperator locator = new PeekStreamOperator(registry, dataSourceNode, QUERY_ID,
        Optional.empty());
    locator.setNewRowCallback(newRowCallback);

    // When:
    locator.open();

    // Then:
    verify(registry, times(1)).register(processingQueueCaptor.capture(), eq(Optional.empty()));
    final ProcessingQueue processingQueue = processingQueueCaptor.getValue();
    processingQueue.offer(row1);
    processingQueue.offer(row2);
    assertThat(locator.next(), is(row1));
    assertThat(locator.next(), is(row2));
    assertThat(locator.next(), nullValue());
    verify(newRowCallback, times(2)).run();
    locator.close();
    verify(registry, times(1)).unregister(processingQueue);
  }

  @Test
  public void shouldDefaultToFalseForHasErrorOnQueue() {
    // Given:
    final PeekStreamOperator locator = new PeekStreamOperator(registry, dataSourceNode, QUERY_ID,
        Optional.empty());
    // When:
    locator.open();

    // Then:
    assertThat(locator.hasError(),is(false));
  }
}
