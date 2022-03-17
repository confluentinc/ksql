/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.scalablepush.locator;

import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.HostInfo;

/**
 * This locator contacts all hosts since there currently isn't a mechanism to find which hosts own
 * a particular persistent query task.
 */
public class AllHostsLocator implements PushLocator {

  private final Supplier<List<PersistentQueryMetadata>> allPersistentQueries;
  private final URL localhost;

  public AllHostsLocator(
      final Supplier<List<PersistentQueryMetadata>> allPersistentQueries,
      final URL localhost
  ) {
    this.allPersistentQueries = allPersistentQueries;
    try {
      this.localhost = new URL(localhost.toString());
    } catch (final MalformedURLException fatalError) {
      throw new IllegalStateException("Could not deep copy URL: " + localhost);
    }
  }


  public List<KsqlNode> locate() {
    final List<PersistentQueryMetadata> currentQueries = allPersistentQueries.get();
    if (currentQueries.isEmpty()) {
      return Collections.emptyList();
    }

    return currentQueries.stream()
        .map(QueryMetadata::getAllStreamsHostMetadata)
        .filter(Objects::nonNull)
        .flatMap(Collection::stream)
        .map(StreamsMetadata::hostInfo)
        .map(hi -> new Node(isLocalhost(hi), buildLocation(hi)))
        .distinct()
        .collect(Collectors.toList());
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

    Node(final boolean isLocal, final URI location) {
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
