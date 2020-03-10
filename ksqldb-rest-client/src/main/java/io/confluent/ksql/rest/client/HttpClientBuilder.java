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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.rest.client.ssl.DefaultSslClientConfigurer;
import io.confluent.ksql.rest.client.ssl.SslClientConfigurer;
import io.confluent.rest.validation.JacksonMessageBodyProvider;
import java.util.Map;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

final class HttpClientBuilder {

  private HttpClientBuilder() {
  }

  static Client buildClient(final Map<String, String> clientProps) {
    return buildClient(ClientBuilder.newBuilder(), new DefaultSslClientConfigurer(), clientProps);
  }

  @VisibleForTesting
  static Client buildClient(
      final javax.ws.rs.client.ClientBuilder clientBuilder,
      final SslClientConfigurer sslClientConfigurer,
      final Map<String, String> clientProps
  ) {
    try {
      clientBuilder.register(new JacksonMessageBodyProvider(JsonMapper.INSTANCE.mapper));

      sslClientConfigurer.configureSsl(clientBuilder, clientProps);

      return clientBuilder.build();
    } catch (final Exception e) {
      throw new KsqlRestClientException("Failed to configure rest client", e);
    }
  }
}
