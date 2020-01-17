package io.confluent.ksql.api.tck;

import io.confluent.ksql.api.server.InsertsPublisherOld;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;

public class InsertsPublisherVerificationTest extends PublisherVerification<JsonObject> {

  private final Vertx vertx;

  public InsertsPublisherVerificationTest() {
    super(new TestEnvironment());
    this.vertx = Vertx.vertx();
  }

  @Override
  public Publisher<JsonObject> createPublisher(long elements) {
    System.out.println("Creating publisher " + elements);
    InsertsPublisherOld publisher = new InsertsPublisherOld(vertx.getOrCreateContext(), elements);
    if (elements < Integer.MAX_VALUE) {
      for (long l = 0; l < elements; l++) {
        publisher.receiveRow(generateRow(l));
      }
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
