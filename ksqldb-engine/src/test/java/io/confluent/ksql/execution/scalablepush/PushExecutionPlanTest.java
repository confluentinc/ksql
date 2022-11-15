package io.confluent.ksql.execution.scalablepush;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.common.QueryRow;
import io.confluent.ksql.execution.common.QueryRowImpl;
import io.confluent.ksql.execution.common.operators.AbstractPhysicalOperator;
import io.confluent.ksql.execution.scalablepush.operators.PushDataSourceOperator;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConstants.QuerySourceType;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
public class PushExecutionPlanTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder().build();
  private static final QueryRow ROW1 = QueryRowImpl.of(SCHEMA, GenericKey.genericKey(),
      Optional.empty(), GenericRow.fromList(ImmutableList.of(1, "abc")), 0);
  private static final QueryRow ROW2 = QueryRowImpl.of(SCHEMA, GenericKey.genericKey(),
      Optional.empty(), GenericRow.fromList(ImmutableList.of(2, "def")), 0);
  private static final String CATCHUP_CONSUMER_GROUP = "foo";

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
  @Mock
  private QuerySourceType querySourceType;
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
        CATCHUP_CONSUMER_GROUP, scalablePushRegistry, pushDataSourceOperator, context,
        querySourceType);
    doNothing().when(pushDataSourceOperator).setNewRowCallback(runnableCaptor.capture());
    when(pushDataSourceOperator.droppedRows()).thenReturn(false);

    final TestSubscriber<QueryRow> subscriber = new TestSubscriber<>();
    pushPhysicalPlan.subscribeAndExecute(Optional.of(subscriber));

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
        CATCHUP_CONSUMER_GROUP, scalablePushRegistry, pushDataSourceOperator, context,
        querySourceType);
    doNothing().when(pushDataSourceOperator).setNewRowCallback(runnableCaptor.capture());
    when(pushDataSourceOperator.droppedRows()).thenReturn(false, false, true);

    final TestSubscriber<QueryRow> subscriber = new TestSubscriber<>();
    pushPhysicalPlan.subscribeAndExecute(Optional.of(subscriber));

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

  @Test
  public void shouldStopOnHasError() throws InterruptedException {
    final PushPhysicalPlan pushPhysicalPlan = new PushPhysicalPlan(root, logicalSchema, queryId,
        CATCHUP_CONSUMER_GROUP, scalablePushRegistry, pushDataSourceOperator, context,
        querySourceType);
    doNothing().when(pushDataSourceOperator).setNewRowCallback(runnableCaptor.capture());
    when(pushDataSourceOperator.hasError()).thenReturn(false, false, true);

    final TestSubscriber<QueryRow> subscriber = new TestSubscriber<>();
    pushPhysicalPlan.subscribeAndExecute(Optional.of(subscriber));

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

    assertThat(subscriber.getError().getMessage(), containsString("Internal error occurred"));
    assertThat(subscriber.getValues().size(), is(1));
    assertThat(subscriber.getValues().get(0), is(ROW1));
    assertThat(pushPhysicalPlan.isClosed(), is(true));
  }

  @Test
  public void shouldThrowErrorOnOpen() throws InterruptedException {
    final PushPhysicalPlan pushPhysicalPlan = new PushPhysicalPlan(root, logicalSchema, queryId,
        CATCHUP_CONSUMER_GROUP, scalablePushRegistry, pushDataSourceOperator, context,
        querySourceType);
    doNothing().when(pushDataSourceOperator).setNewRowCallback(runnableCaptor.capture());
    doThrow(new RuntimeException("Error on open")).when(root).open();

    final TestSubscriber<QueryRow> subscriber = new TestSubscriber<>();
    pushPhysicalPlan.subscribeAndExecute(Optional.of(subscriber));

    while (subscriber.getError() == null) {
      Thread.sleep(100);
    }
    assertThat(subscriber.getError().getMessage(), containsString("Error on open"));
  }

  @Test
  public void shouldThrowErrorOnNext() throws InterruptedException {
    final PushPhysicalPlan pushPhysicalPlan = new PushPhysicalPlan(root, logicalSchema, queryId,
        CATCHUP_CONSUMER_GROUP, scalablePushRegistry, pushDataSourceOperator, context,
        querySourceType);
    doNothing().when(pushDataSourceOperator).setNewRowCallback(runnableCaptor.capture());
    when(pushDataSourceOperator.droppedRows()).thenReturn(false);
    doThrow(new RuntimeException("Error on next")).when(root).next();

    final TestSubscriber<QueryRow> subscriber = new TestSubscriber<>();
    pushPhysicalPlan.subscribeAndExecute(Optional.of(subscriber));

    while (subscriber.getError() == null) {
      Thread.sleep(100);
    }
    assertThat(subscriber.getError().getMessage(), containsString("Error on next"));
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
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
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

    @SuppressFBWarnings(value = "EI_EXPOSE_REP")
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
