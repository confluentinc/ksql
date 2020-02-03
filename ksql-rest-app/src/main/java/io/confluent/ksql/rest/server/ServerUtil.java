/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;

import io.confluent.ksql.util.KsqlException;
import io.confluent.rest.RestConfig;
import java.net.URI;
import java.net.URL;
import java.util.List;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.processor.internals.StreamsMetadataState;
import org.apache.kafka.streams.state.HostInfo;

public final class ServerUtil {
  
  private ServerUtil() {
  }
  
  public static URI getServerAddress(final KsqlRestConfig restConfig) {
    final List<String> listeners = restConfig.getList(RestConfig.LISTENERS_CONFIG);
    final String address = listeners.stream()
        .map(String::trim)
        .findFirst()
        .orElseThrow(() ->
            new ConfigException(RestConfig.LISTENERS_CONFIG, listeners, "value cannot be empty"));

    try {
      return new URL(address).toURI();
    } catch (final Exception e) {
      throw new ConfigException(RestConfig.LISTENERS_CONFIG, listeners, e.getMessage());
    }
  }

  public static HostInfo parseHostInfo(final String applicationServerId) {
    if (applicationServerId == null || applicationServerId.trim().isEmpty()) {
      return StreamsMetadataState.UNKNOWN_HOST;
    }
    final String host = getHost(applicationServerId);
    final Integer port = getPort(applicationServerId);

    if (host == null || port == null) {
      throw new KsqlException(String.format(
          "Error parsing host address %s. Expected format host:port.", applicationServerId));
    }

    return new HostInfo(host, port);
  }

  /**
   * Constructs a URI for the remote node in the cluster, using the same protocol as localhost.
   * @param localHost Local URL from which to take protocol
   * @param remoteHost The remote host
   * @param remotePort The remote port
   * @return uri
   */
  static URI buildRemoteUri(final URL localHost, final String remoteHost, final int remotePort) {
    try {
      return new URL(localHost.getProtocol(), remoteHost, remotePort, "/").toURI();
    } catch (final Exception e) {
      throw new IllegalStateException("Failed to convert remote host info to URL."
                                          + " remoteInfo: " + remoteHost + ":" + remotePort);
    }
  }
}
