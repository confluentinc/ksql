package io.confluent.ksql.rest.client;

import static io.confluent.ksql.rest.client.StreamPublisher.toJsonMsg;

import io.confluent.ksql.reactive.BufferedPublisher;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.parsetools.RecordParser;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class QueryStreamPublisher<T> extends BufferedPublisher<T> {

  private final HttpClientResponse response;
  private boolean drainHandlerSet;

  public QueryStreamPublisher(final Context context, final HttpClientResponse response,
      final Function<Buffer, T> mapper,
      final CompletableFuture<ResponseWithBody> bodyFuture) {
    super(context);
    this.response = response;
    final RecordParser recordParser = RecordParser.newDelimited("\n", response);
    recordParser.exceptionHandler(bodyFuture::completeExceptionally)
        .handler(buff -> {
          if (buff.length() == 0) {
            // Ignore empty buffer - the server can insert random newlines!
            return;
          }
          final Buffer jsonMsg = toJsonMsg(buff, false);
          if (!accept(mapper.apply(jsonMsg))) {
            if (!drainHandlerSet) {
              recordParser.pause();
              drainHandlerSet = true;
              drainHandler(() -> {
                drainHandlerSet = false;
                recordParser.resume();
              });
            }
          }
        })
        .endHandler(v -> complete());
  }

  public void close() {
    response.request().connection().close();
  }
}
