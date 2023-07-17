package io.confluent.ksql.api.server;

import static io.confluent.ksql.rest.Errors.ERROR_CODE_TOO_MANY_REQUESTS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import io.confluent.ksql.util.KsqlRateLimitException;
import io.vertx.ext.web.RoutingContext;
import java.util.concurrent.CompletionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ServerUtilsTest {
  @Mock RoutingContext routingContext;

  @Test
  public void testHandleServerRateLimitException() {
    String ratelimitErrorMsg = "Host is at rate limit for pull queries. Currently set to 1 QPS";
    CompletionException ratelimitException = new CompletionException(
        new KsqlRateLimitException(ratelimitErrorMsg));
    ServerUtils.handleEndpointException(ratelimitException, routingContext, "test msg");
    verify(routingContext).fail(eq(TOO_MANY_REQUESTS.code()), argThat(x -> {
      assertThat(x.getMessage(), equalTo(ratelimitErrorMsg));
      assertThat(((KsqlApiException) x).getErrorCode(), equalTo(ERROR_CODE_TOO_MANY_REQUESTS));
      return true;
    }));
  }
}
