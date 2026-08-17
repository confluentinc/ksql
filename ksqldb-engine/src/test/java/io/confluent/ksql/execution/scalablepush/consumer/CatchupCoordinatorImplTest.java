package io.confluent.ksql.execution.scalablepush.consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CatchupCoordinatorImplTest {

  private ExecutorService executorService;

  @Before
  public void setUp() {
    executorService = Executors.newFixedThreadPool(3);
  }

  @After
  public void tearDown() {
    executorService.shutdownNow();
  }

  @Test
  public void shouldSwitchOver_oneThread() {
    // Given:
    final CatchupCoordinatorImpl catchupCoordinator = new CatchupCoordinatorImpl();
    final AtomicBoolean signalledLatest = new AtomicBoolean();
    final AtomicBoolean switchOver = new AtomicBoolean();
    final AtomicBoolean latestDone = new AtomicBoolean();

    // When:
    executorService.submit(() -> {
      while (!switchOver.get()) {
        catchupCoordinator.checkShouldWaitForCatchup();
        sleep();
      }
      latestDone.set(true);
    });

    // That:
    while (!switchOver.get()) {
      catchupCoordinator.checkShouldCatchUp(
          signalledLatest, soft -> true, () -> switchOver.set(true));
      sleep();
    }
    assertThatEventually(latestDone::get, is(true));
  }

  @Test
  public void shouldNotSwitchOver_notCaughtUp() {
    // Given:
    final CatchupCoordinatorImpl catchupCoordinator = new CatchupCoordinatorImpl();
    final AtomicBoolean signalledLatest = new AtomicBoolean();
    final AtomicBoolean switchOver = new AtomicBoolean();
    final AtomicBoolean catchupDone = new AtomicBoolean();
    final AtomicBoolean latestDone = new AtomicBoolean();
    final long timeMs = System.currentTimeMillis();


    // When:
    executorService.submit(() -> {
      while (!switchOver.get() && !catchupDone.get()) {
        catchupCoordinator.checkShouldWaitForCatchup();
        sleep();
      }
      latestDone.set(true);
    });

    // That:
    while (!switchOver.get()) {
      catchupCoordinator.checkShouldCatchUp(
          signalledLatest, soft -> false, () -> switchOver.set(true));
      sleep();

      if (System.currentTimeMillis() - timeMs > 1000) {
        break;
      }
    }
    catchupDone.set(true);
    assertThat(switchOver.get(), is(false));
    assertThatEventually(latestDone::get, is(true));
  }

  @Test
  public void shouldSwitchOver_threeThreads() {
    // Given:
    final CatchupCoordinatorImpl catchupCoordinator = new CatchupCoordinatorImpl();
    final AtomicBoolean signalledLatest1 = new AtomicBoolean();
    final AtomicBoolean signalledLatest2 = new AtomicBoolean();
    final AtomicBoolean signalledLatest3 = new AtomicBoolean();
    final AtomicBoolean switchOver1 = new AtomicBoolean();
    final AtomicBoolean switchOver2 = new AtomicBoolean();
    final AtomicBoolean switchOver3 = new AtomicBoolean();
    final AtomicBoolean latestDone = new AtomicBoolean();

    // When:
    executorService.submit(() -> {
      while (!(switchOver1.get() && switchOver2.get() && switchOver3.get())) {
        catchupCoordinator.checkShouldWaitForCatchup();
        sleep();
      }
      latestDone.set(true);
    });

    // That:
    while (!(switchOver1.get() && switchOver2.get() && switchOver3.get())) {
      if (!switchOver1.get()) {
        catchupCoordinator.checkShouldCatchUp(
            signalledLatest1, soft -> true, () -> switchOver1.set(true));
      }
      if (!switchOver2.get()) {
        catchupCoordinator.checkShouldCatchUp(
            signalledLatest2, soft -> true, () -> switchOver2.set(true));
      }
      if (!switchOver3.get()) {
        catchupCoordinator.checkShouldCatchUp(
            signalledLatest3, soft -> true, () -> switchOver3.set(true));
      }
      sleep();
    }
    assertThatEventually(latestDone::get, is(true));
  }

  private void sleep() {
    try {
      Thread.sleep(100L);
    } catch (InterruptedException e) {
    }
  }

  @Test
  public void shouldWakeWaitingLatestPromptlyWhenCatchupClosesWithoutJoining()
      throws InterruptedException {
    // Given: a latest thread parked in checkShouldWaitForCatchup after a
    // catchup has signalledLatest but before it ever catches up. The catchup
    // then closes abnormally without joining (catchupIsClosing path).
    //
    // Without the notifyAll() in catchupIsClosing the latest thread waits the
    // full WAIT_TIME_MS (10s), stalling scalable-push delivery for every
    // client whenever a catchup connection drops mid-flight.
    final CatchupCoordinatorImpl coordinator = new CatchupCoordinatorImpl();
    final AtomicBoolean signalledLatest = new AtomicBoolean();
    final AtomicBoolean latestReturned = new AtomicBoolean();
    final java.util.concurrent.CountDownLatch latestEntered =
        new java.util.concurrent.CountDownLatch(1);

    // Catchup signals latest via the soft-caught-up branch (catchupJoiners -> 1)
    // but is NOT hard-caught-up, so it never takes the join-and-switch-over
    // path and stays "signalled but not joined".
    coordinator.checkShouldCatchUp(signalledLatest, soft -> soft, () -> { });
    assertThat(signalledLatest.get(), is(true));

    // Latest enters the wait loop in a background thread.
    executorService.submit(() -> {
      latestEntered.countDown();
      coordinator.checkShouldWaitForCatchup();
      latestReturned.set(true);
    });
    latestEntered.await();
    // Give the latest thread a moment to actually enter wait().
    Thread.sleep(200L);

    // When: the catchup closes without ever joining.
    final long beforeMs = System.currentTimeMillis();
    coordinator.catchupIsClosing(signalledLatest);

    // Then: latest wakes promptly, well under the 10s WAIT_TIME_MS.
    assertThatEventually(latestReturned::get, is(true));
    final long elapsedMs = System.currentTimeMillis() - beforeMs;
    assertThat(
        "latest must wake on catchupIsClosing, not on the 10s timeout; took " + elapsedMs + "ms",
        elapsedMs < 5000, is(true));
  }
}
