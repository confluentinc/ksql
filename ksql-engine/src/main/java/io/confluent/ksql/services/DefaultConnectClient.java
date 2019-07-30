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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;
import org.apache.http.HttpStatus;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default implementation of {@code ConnectClient}. This implementation is
 * thread safe, and the methods are all <i>blocking</i> and are configured with
 * default timeouts of {@value #DEFAULT_TIMEOUT_MS}ms.
 */
public class DefaultConnectClient implements ConnectClient {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultConnectClient.class);
  private static final ObjectMapper MAPPER = JsonMapper.INSTANCE.mapper;

  private static final String CONNECTORS = "/connectors";
  private static final int DEFAULT_TIMEOUT_MS = 5_000;

  private final URI connectUri;

  public DefaultConnectClient(final String connectUri) {
    Objects.requireNonNull(connectUri, "connectUri");

    try {
      this.connectUri = new URI(connectUri);
    } catch (URISyntaxException e) {
      throw new KsqlException(
          "Could not initialize connect client due to invalid URI: " + connectUri, e);
    }
  }

  @Override
  public ConnectResponse<ConnectorInfo> create(
      final String connector,
      final Map<String, String> config
  ) {
    try {
      LOG.debug("Issuing request to Kafka Connect at URI {} with name {} and config {}",
          connectUri,
          connector,
          config);

      final ConnectResponse<ConnectorInfo> connectResponse = Request
          .Post(connectUri.resolve(CONNECTORS))
          .socketTimeout(DEFAULT_TIMEOUT_MS)
          .connectTimeout(DEFAULT_TIMEOUT_MS)
          .bodyString(
              MAPPER.writeValueAsString(
                  ImmutableMap.of(
                      "name", connector,
                      "config", config)),
              ContentType.APPLICATION_JSON
          )
          .execute()
          .handleResponse(createHandler(HttpStatus.SC_CREATED, ConnectorInfo.class));

      connectResponse.error()
          .ifPresent(error -> LOG.warn("Did not CREATE connector {}: {}", connector, error));

      return connectResponse;
    } catch (final Exception e) {
      throw new KsqlServerException(e);
    }
  }

  private static <T> ResponseHandler<ConnectResponse<T>> createHandler(
      final int expectedStatus,
      final Class<T> entityClass
  ) {
    return httpResponse -> {
      if (httpResponse.getStatusLine().getStatusCode() != expectedStatus) {
        final String entity = EntityUtils.toString(httpResponse.getEntity());
        return ConnectResponse.of(entity);
      }

      final T info = MAPPER.readValue(
          httpResponse.getEntity().getContent(),
          entityClass);

      return ConnectResponse.of(info);
    };
  }
}
