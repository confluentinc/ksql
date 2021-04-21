package io.confluent.ksql.physical.scalable_push.locator;

import java.net.URI;
import java.util.Set;
import org.apache.kafka.streams.state.HostInfo;

public interface PushLocator {

  Set<KsqlNode> locate();

  interface KsqlNode {

    /**
     * @return {@code true} if this is the local node, i.e. the KSQL instance handling the call.
     */
    boolean isLocal();

    /**
     * @return The base URI of the node, including protocol, host and port.
     */
    URI location();
  }
}
