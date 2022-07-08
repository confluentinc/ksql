package io.confluent.ksql.rest.util;

import io.confluent.ksql.util.MockSystemExit;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.log4j.LogManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


@RunWith(MockitoJUnitRunner.class)
public class KsqlUncaughtExceptionHandlerTest {
  @Mock
  public StreamThread streamThread;

  @Test
  public void shouldNotSystemExitWhenStreamThreadThrowsAnError() throws InterruptedException {
    // When
    final CountDownLatch latch = new CountDownLatch(1);
    KsqlUncaughtExceptionHandler handler = new KsqlUncaughtExceptionHandler(LogManager::shutdown, Optional.of(latch));
    handler.uncaughtExceptionInternal(streamThread, new Exception(), new MockSystemExit());
    // Then
    assertThat(latch.await(60000, TimeUnit.MILLISECONDS), is(true));
  }
}
