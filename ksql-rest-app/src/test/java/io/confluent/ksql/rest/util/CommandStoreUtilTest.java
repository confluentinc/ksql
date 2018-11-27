package io.confluent.ksql.rest.util;


import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.computation.ReplayableCommandQueue;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.core.Response;
import org.eclipse.jetty.http.HttpStatus.Code;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CommandStoreUtilTest {

  private static final long TIMEOUT = 5000L;
  private static final long OFFSET = 2;

  @Mock
  private ReplayableCommandQueue replayableCommandQueue;
  @Mock
  private KsqlRequest request;

  @Test
  public void shouldNotWaitIfNoOffsetSpecified() throws Exception {
    // Given:
    when(request.getCommandOffset()).thenReturn(Optional.empty());

    // When:
    CommandStoreUtil.waitForCommandOffset(replayableCommandQueue, request, TIMEOUT);

    // Then:
    verify(replayableCommandQueue, never()).ensureConsumedUpThrough(anyLong(), anyLong());
  }

  @Test
  public void shouldWaitIfOffsetSpecified() throws Exception {
    // Given:
    when(request.getCommandOffset()).thenReturn(Optional.of(OFFSET));

    // When:
    CommandStoreUtil.waitForCommandOffset(replayableCommandQueue, request, TIMEOUT);

    // Then:
    verify(replayableCommandQueue).ensureConsumedUpThrough(OFFSET, TIMEOUT);
  }

  @Test
  public void shouldThrowKsqlRestExceptionOnTimeout() throws Exception {
    // Given:
    when(request.getCommandOffset()).thenReturn(Optional.of(OFFSET));
    doThrow(new TimeoutException("uh oh"))
        .when(replayableCommandQueue).ensureConsumedUpThrough(OFFSET, TIMEOUT);

    try {
      // When:
      CommandStoreUtil.httpWaitForCommandOffset(replayableCommandQueue, request, TIMEOUT);

      // Then:
      fail("Should propagate error.");
    } catch (final KsqlRestException e) {
      final Response response = e.getResponse();
      assertThat(response.getStatus(), is(Code.SERVICE_UNAVAILABLE.getCode()));
      assertThat(response.getEntity(), is(instanceOf(KsqlErrorMessage.class)));
      final KsqlErrorMessage message = (KsqlErrorMessage) (response.getEntity());
      assertThat(message.getMessage(), is("uh oh"));
    }
  }
}