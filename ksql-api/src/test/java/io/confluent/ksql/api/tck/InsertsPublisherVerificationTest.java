package io.confluent.ksql.api.tck;

import io.confluent.ksql.api.server.InsertsPublisher;
import io.vertx.core.json.JsonObject;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;

public class InsertsPublisherVerificationTest extends PublisherVerification<JsonObject> {

  public InsertsPublisherVerificationTest() {
    super(new TestEnvironment());
  }

  @Override
  public Publisher<JsonObject> createPublisher(long elements) {
    InsertsPublisher publisher = new InsertsPublisher();
    for (long l = 0; l < elements; l++) {
      publisher.receiveRow(generateRow(l));
    }
    return publisher;
  }

  @Override
  public Publisher<JsonObject> createFailedPublisher() {
    return null;
  }

  private JsonObject generateRow(long num) {
    return new JsonObject().put("id", num).put("foo", "bar");
  }
}
