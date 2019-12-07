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

package io.confluent.ksql.rest.client;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.parser.json.KsqlTypesDeserializationModule;
import io.confluent.ksql.properties.LocalProperties;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

@SuppressWarnings("WeakerAccess") // Public API
public final class KsqlClient implements AutoCloseable {

  static {
    JsonMapper.INSTANCE.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    JsonMapper.INSTANCE.mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
    JsonMapper.INSTANCE.mapper.registerModule(new KsqlTypesDeserializationModule(false));
  }

  private final Client httpClient;
  private final LocalProperties localProperties;

  public KsqlClient(
      final Map<String, String> clientProps,
      final Optional<BasicCredentials> credentials,
      final LocalProperties localProperties
  ) {
    this(HttpClientBuilder.buildClient(clientProps), credentials, localProperties);
  }

  @VisibleForTesting
  KsqlClient(
      final Client httpClient,
      final Optional<BasicCredentials> credentials,
      final LocalProperties localProperties
  ) {
    this.httpClient = requireNonNull(httpClient, "httpClient");
    this.localProperties = requireNonNull(localProperties, "localProperties");

    credentials.ifPresent(creds -> {
      httpClient.register(HttpAuthenticationFeature.basic(creds.username(), creds.password()));
    });
  }

  public KsqlTarget target(final URI server) {
    final WebTarget target = httpClient.target(server);
    return new KsqlTarget(target, localProperties, Optional.empty());
  }

  public void close() {
    httpClient.close();
  }
}
