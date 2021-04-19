package io.confluent.ksql.physical.scalable_push.locator;

import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.execution.streams.materialization.ks.KsLocator;
import io.confluent.ksql.execution.streams.materialization.ks.KsLocator.Node;
import io.confluent.ksql.util.KsqlHostInfo;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;

public class AllHostsLocator implements PushLocator {

  private final Supplier<List<PersistentQueryMetadata>> allPersistentQueries;
  private final URL localhost;

  public AllHostsLocator(final Supplier<List<PersistentQueryMetadata>> allPersistentQueries,
      final URL localhost) {
    this.allPersistentQueries = allPersistentQueries;
    this.localhost = localhost;
  }


  public Set<KsqlNode> locate() {
    final List<PersistentQueryMetadata> currentQueries = allPersistentQueries.get();
    if (currentQueries.isEmpty()) {
      return Collections.emptySet();
    }

    return currentQueries.stream()
        .map(QueryMetadata::getAllMetadata)
        .filter(Objects::nonNull)
        .flatMap(Collection::stream)
        .filter(streamsMetadata -> streamsMetadata != StreamsMetadata.NOT_AVAILABLE)
        .map(StreamsMetadata::hostInfo)
        .map(hi -> new Node(isLocalhost(hi), buildLocation(hi)))
        .collect(Collectors.toSet());
  }

  private boolean isLocalhost(final HostInfo hostInfo) {
    if (hostInfo.port() != localhost.getPort()) {
      return false;
    }

    return hostInfo.host().equalsIgnoreCase(localhost.getHost())
        || hostInfo.host().equalsIgnoreCase("localhost");
  }

  private URI buildLocation(final HostInfo remoteInfo) {
    try {
      return new URL(
          localhost.getProtocol(),
          remoteInfo.host(),
          remoteInfo.port(),
          "/"
      ).toURI();
    } catch (final Exception e) {
      throw new IllegalStateException("Failed to convert remote host info to URL."
          + " remoteInfo: " + remoteInfo);
    }
  }

  private static class Node implements KsqlNode {

    private final boolean isLocal;
    private final URI location;

    public Node(final boolean isLocal, final URI location) {
      this.isLocal = isLocal;
      this.location = location;
    }

    @Override
    public boolean isLocal() {
      return isLocal;
    }

    @Override
    public URI location() {
      return location;
    }

    @Override
    public String toString() {
      return "Node{"
          + "isLocal = " + isLocal
          + ", location = " + location
          + "}";
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final Node that = (Node) o;
      return isLocal == that.isLocal
          && location.equals(that.location);
    }

    @Override
    public int hashCode() {
      return Objects.hash(isLocal, location);
    }
  }
}
