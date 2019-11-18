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

import io.confluent.rest.RestConfig;
import java.net.URI;
import java.net.URL;
import java.util.List;
import org.apache.kafka.common.config.ConfigException;

public final class ServerUtil {
  
  private ServerUtil() {
  }
  
  public static URI getServerAddress(final KsqlRestConfig restConfig) {
    final List<String> listeners = restConfig.getList(RestConfig.LISTENERS_CONFIG);
    final String address = listeners.stream()
            .map(String::trim)
            .findFirst()
            .orElseThrow(() -> invalidAddressException(listeners, "value cannot be empty"));

    try {
      return new URL(address).toURI();
    } catch (final Exception e) {
      throw invalidAddressException(listeners, e.getMessage());
    }
  }

  private static RuntimeException invalidAddressException(
          final List<String> serverAddresses,
          final String message
  ) {
    return new ConfigException(RestConfig.LISTENERS_CONFIG, serverAddresses, message);
  }
}
