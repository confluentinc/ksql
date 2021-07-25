package io.confluent.ksql.physical.scalablepush;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.physical.common.operators.AbstractPhysicalOperator;
import io.confluent.ksql.physical.scalablepush.operators.PushDataSourceOperator;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@RunWith(MockitoJUnitRunner.class)
public class PushPhysicalPlanTest {

  private static final List<?> ROW1 = ImmutableList.of(1, "abc");
  private static final List<?> ROW2 = ImmutableList.of(2, "def");

  @Mock
  private AbstractPhysicalOperator root;
  @Mock
  private LogicalSchema logicalSchema;
  @Mock
  private QueryId queryId;
  @Mock
  private ScalablePushRegistry scalablePushRegistry;
  @Mock
  private PushDataSourceOperator pushDataSourceOperator;
  @Captor
  private ArgumentCaptor<Runnable> runnableCaptor;

  private Vertx vertx;
  private Context context;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    context = vertx.getOrCreateContext();
  }

  @After
  public void tearDown() {
    if (vertx != null) {
      vertx.close();
    }
  }

  @Test
  public void shouldPublishRows() throws InterruptedException {
    final PushPhysicalPlan pushPhysicalPlan = new PushPhysicalPlan(root, logicalSchema, queryId,
        scalablePushRegistry, pushDataSourceOperator, context);
    doNothing().when(pushDataSourceOperator).setNewRowCallback(runnableCaptor.capture());
    when(pushDataSourceOperator.droppedRows()).thenReturn(false);

    final BufferedPublisher<List<?>> publisher = pushPhysicalPlan.execute();
    final TestSubscriber<List<?>> subscriber = new TestSubscriber<>();
    publisher.subscribe(subscriber);

    context.owner().setPeriodic(50, timerId -> {
      if (runnableCaptor.getValue() == null) {
        return;
      }
      when(root.next()).thenReturn(ROW1, ROW2, null);

      runnableCaptor.getValue().run();
      runnableCaptor.getValue().run();

      context.owner().cancelTimer(timerId);
    });

    while (subscriber.getValues().size() < 2) {
      Thread.sleep(100);
    }
  }

  @Test
  public void shouldStopOnDroppedRows() throws InterruptedException {
    final PushPhysicalPlan pushPhysicalPlan = new PushPhysicalPlan(root, logicalSchema, queryId,
        scalablePushRegistry, pushDataSourceOperator, context);
    doNothing().when(pushDataSourceOperator).setNewRowCallback(runnableCaptor.capture());
    when(pushDataSourceOperator.droppedRows()).thenReturn(false, true);

    final BufferedPublisher<List<?>> publisher = pushPhysicalPlan.execute();
    final TestSubscriber<List<?>> subscriber = new TestSubscriber<>();
    publisher.subscribe(subscriber);

    context.owner().setPeriodic(50, timerId -> {
      if (runnableCaptor.getValue() == null) {
        return;
      }
      when(root.next()).thenReturn(ROW1, ROW2, null);

      runnableCaptor.getValue().run();
      runnableCaptor.getValue().run();

      context.owner().cancelTimer(timerId);
    });

    while (subscriber.getError() == null) {
      Thread.sleep(100);
    }

    assertThat(subscriber.getError().getMessage(), containsString("Dropped rows"));
    assertThat(subscriber.getValues().size(), is(1));
    assertThat(subscriber.getValues().get(0), is(ROW1));
    assertThat(pushPhysicalPlan.isClosed(), is(true));
  }

  public static class TestSubscriber<T> implements Subscriber<T> {

    private Subscription sub;
    private boolean completed;
    private Throwable error;
    private final List<T> values = new ArrayList<>();

    public TestSubscriber() {
    }

    @Override
    public synchronized void onSubscribe(final Subscription sub) {
      this.sub = sub;
      sub.request(1);
    }

    @Override
    public synchronized void onNext(final T value) {
      values.add(value);
      sub.request(1);
    }

    @Override
    public synchronized void onError(final Throwable t) {
      this.error = t;
    }

    @Override
    public synchronized void onComplete() {
      this.completed = true;
    }

    public synchronized boolean isCompleted() {
      return completed;
    }

    public synchronized Throwable getError() {
      return error;
    }

    public synchronized List<T> getValues() {
      return ImmutableList.copyOf(values);
    }

    public synchronized Subscription getSub() {
      return sub;
    }
  }
}
