package io.confluent.ksql.physical.scalablepush;

import io.confluent.ksql.physical.scalablepush.PushRouting.PushConnectionsHandle;
import java.util.concurrent.CompletableFuture;

public interface PushQueryQueuePopulator {

  CompletableFuture<PushConnectionsHandle> run();
}
