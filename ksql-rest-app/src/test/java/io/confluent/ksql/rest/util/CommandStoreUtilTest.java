package io.confluent.ksql.rest.util;


import static io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers.errorMessage;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionKsqlErrorMessage;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionStatusCode;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import org.eclipse.jetty.http.HttpStatus.Code;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CommandStoreUtilTest {

  private static final Duration TIMEOUT = Duration.ofMillis(5000L);
  private static final long SEQUENCE_NUMBER = 2;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private CommandQueue commandQueue;
  @Mock
  private KsqlRequest request;

  @Test
  public void shouldNotWaitIfNoSequenceNumberSpecified() throws Exception {
    // Given:
    when(request.getCommandSequenceNumber()).thenReturn(Optional.empty());

    // When:
    CommandStoreUtil.waitForCommandSequenceNumber(commandQueue, request, TIMEOUT);

    // Then:
    verify(commandQueue, never()).ensureConsumedPast(anyLong(), any());
  }

  @Test
  public void shouldWaitIfSequenceNumberSpecified() throws Exception {
    // Given:
    when(request.getCommandSequenceNumber()).thenReturn(Optional.of(SEQUENCE_NUMBER));

    // When:
    CommandStoreUtil.waitForCommandSequenceNumber(commandQueue, request, TIMEOUT);

    // Then:
    verify(commandQueue).ensureConsumedPast(SEQUENCE_NUMBER, TIMEOUT);
  }

  @Test
  public void shouldThrowKsqlRestExceptionOnTimeout() throws Exception {
    // Given:
    when(request.getCommandSequenceNumber()).thenReturn(Optional.of(SEQUENCE_NUMBER));
    doThrow(new TimeoutException("uh oh"))
        .when(commandQueue).ensureConsumedPast(SEQUENCE_NUMBER, TIMEOUT);

    // Expect:
    expectedException.expect(KsqlRestException.class);
    expectedException.expect(exceptionStatusCode(is(Code.SERVICE_UNAVAILABLE)));
    expectedException.expect(exceptionKsqlErrorMessage(errorMessage(is("uh oh"))));

    // When:
    CommandStoreUtil.httpWaitForCommandSequenceNumber(commandQueue, request, TIMEOUT);
  }
}