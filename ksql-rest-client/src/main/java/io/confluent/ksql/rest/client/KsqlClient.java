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
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.rest.client.json.KsqlTypesDeserializationModule;
import java.net.URI;
import java.util.Optional;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;

@SuppressWarnings("WeakerAccess") // Public API
public final class KsqlClient {

  static {
    JsonMapper.INSTANCE.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    JsonMapper.INSTANCE.mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
    JsonMapper.INSTANCE.mapper.registerModule(new KsqlTypesDeserializationModule());
  }

  private final Client sharedClient;
  private final LocalProperties localProperties;

  public KsqlClient(
      final Client sharedClient,
      final LocalProperties localProperties
  ) {
    this.sharedClient = requireNonNull(sharedClient, "sharedClient");
    this.localProperties = requireNonNull(localProperties, "localProperties");
  }

  public KsqlTarget target(final URI server) {
    final WebTarget target = sharedClient.target(server);
    return new KsqlTarget(target, localProperties, Optional.empty());
  }
}
