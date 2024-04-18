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

import io.confluent.ksql.rest.entity.ConfigInfos;
import io.confluent.ksql.rest.entity.ConnectorInfo;
import io.confluent.ksql.rest.entity.ConnectorStateInfo;
import io.confluent.ksql.rest.entity.SimpleConnectorPluginInfo;
import io.confluent.ksql.util.KsqlPreconditions;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * An interface defining the common operations to communicate with
 * a Kafka Connect cluster.
 */
public interface ConnectClient {

  /**
   * List all of the connectors available in this connect cluster.
   *
   * @return a list of connector names
   */
  ConnectResponse<List<String>> connectors();

  /**
   * List all of the connector plugins available in this connect cluster.
   *
   * @return a list of connector plugins
   */
  ConnectResponse<List<SimpleConnectorPluginInfo>> connectorPlugins();

  /**
   * Gets the configuration for a specified connector.
   *
   * @param connector the name of the connector
   */
  ConnectResponse<ConnectorInfo> describe(String connector);

  /**
   * Creates a connector with {@code connector} as the name under the
   * specified configuration.
   *
   * @param connector the name of the connector
   * @param config    the connector configuration
   */
  ConnectResponse<ConnectorInfo> create(String connector, Map<String, String> config);

  /**
   * Validates specified connector configuration for the given plugin.
   *
   * @param plugin  the name of the connector plugin
   * @param config  the connector configuration
   */
  ConnectResponse<ConfigInfos> validate(String plugin, Map<String, String> config);

  /**
   * Get the status of {@code connector}.
   *
   * @param connector the name of the connector
   */
  ConnectResponse<ConnectorStateInfo> status(String connector);

  /**
   * Delete the {@code connector}.
   *
   * @param connector the connector name
   */
  ConnectResponse<String> delete(String connector);

  /**
   * Get the topics the {@code connector} is using.
   *
   * @param connector the connector name
   * @return the topics
   */
  ConnectResponse<Map<String, Map<String, List<String>>>> topics(String connector);

  /**
   * An optionally successful response. Either contains a value of type {@code <T>} or an error,
   * which is the string representation of the response entity.
   */
  class ConnectResponse<T> {

    private final Optional<T> datum;
    private final Optional<String> error;
    private final int httpCode;

    public static <T> ConnectResponse<T> success(final T datum, final int code) {
      return new ConnectResponse<>(datum, null, code);
    }

    public static <T> ConnectResponse<T> failure(final String error, final int code) {
      return new ConnectResponse<>(null, error, code);
    }

    private ConnectResponse(final T datum, final String error, final int code) {
      KsqlPreconditions.checkArgument(
          datum != null ^ error != null,
          "expected exactly one of datum or error to be null"
      );
      this.datum = Optional.ofNullable(datum);
      this.error = Optional.ofNullable(error);
      this.httpCode = code;
    }

    public Optional<T> datum() {
      return datum;
    }

    public Optional<String> error() {
      return error;
    }

    public int httpCode() {
      return httpCode;
    }
  }

}
