package io.confluent.ksql.physical.scalable_push;

import io.confluent.ksql.physical.scalable_push.PushRouting.PushConnectionsHandle;
import java.util.concurrent.CompletableFuture;

public interface PushQueryQueuePopulator {

  CompletableFuture<PushConnectionsHandle> run();
}
