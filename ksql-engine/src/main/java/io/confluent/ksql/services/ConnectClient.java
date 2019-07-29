/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.services;

import io.confluent.ksql.util.KsqlPreconditions;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;

/**
 * An interface defining the common operations to communicate with
 * a Kafka Connect cluster.
 */
public interface ConnectClient {

  /**
   * Creates a connector with {@code connector} as the name under the
   * specified configuration.
   *
   * @param connector the name of the connector
   * @param config    the connector configuration
   */
  ConnectResponse<ConnectorInfo> create(String connector, Map<String, String> config);

  /**
   * An optionally successful response. Either contains a value of type
   * {@code <T>} or an error, which is the string representation of the
   * response entity.
   */
  class ConnectResponse<T> {
    private final Optional<T> datum;
    private final Optional<String> error;

    public static <T> ConnectResponse<T> of(final T datum) {
      return new ConnectResponse<>(datum, null);
    }

    public static <T> ConnectResponse<T> of(final String error) {
      return new ConnectResponse<>(null, error);
    }

    private ConnectResponse(final T datum, final String error) {
      KsqlPreconditions.checkArgument(
          datum != null ^ error != null,
          "expected exactly one of datum or error to be null");
      this.datum = Optional.ofNullable(datum);
      this.error = Optional.ofNullable(error);
    }

    public Optional<T> datum() {
      return datum;
    }

    public Optional<String> error() {
      return error;
    }
  }

}
