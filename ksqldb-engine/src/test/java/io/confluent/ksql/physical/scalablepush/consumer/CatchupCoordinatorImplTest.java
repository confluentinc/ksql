package io.confluent.ksql.physical.scalablepush.consumer;

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

    // When:
    executorService.submit(() -> {
      while (!switchOver.get()) {
        catchupCoordinator.checkShouldWaitForCatchup();
        sleep();
      }
    });

    // That:
    while (!switchOver.get()) {
      catchupCoordinator.checkShouldCatchUp(
          signalledLatest, () -> true, () -> switchOver.set(true));
      sleep();
    }
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

    // When:
    executorService.submit(() -> {
      while (!(switchOver1.get() && switchOver2.get() && switchOver3.get())) {
        catchupCoordinator.checkShouldWaitForCatchup();
        sleep();
      }
    });

    // That:
    while (!(switchOver1.get() && switchOver2.get() && switchOver3.get())) {
      if (!switchOver1.get()) {
        catchupCoordinator.checkShouldCatchUp(
            signalledLatest1, () -> true, () -> switchOver1.set(true));
      }
      if (!switchOver2.get()) {
        catchupCoordinator.checkShouldCatchUp(
            signalledLatest2, () -> true, () -> switchOver2.set(true));
      }
      if (!switchOver3.get()) {
        catchupCoordinator.checkShouldCatchUp(
            signalledLatest3, () -> true, () -> switchOver3.set(true));
      }
      sleep();
    }
  }

  private void sleep() {
    try {
      Thread.sleep(100L);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
