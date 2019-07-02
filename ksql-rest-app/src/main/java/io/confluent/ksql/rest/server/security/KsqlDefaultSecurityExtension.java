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

package io.confluent.ksql.rest.server.security;

import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;

/**
 * This is the default security extension for KSQL. For now, this class is just a dummy
 * implementation of the {@link KsqlSecurityExtension} interface.
 */
public class KsqlDefaultSecurityExtension implements KsqlSecurityExtension {

  @Override
  public void initialize(final KsqlConfig ksqlConfig) {
  }

  @Override
  public Optional<KsqlAuthorizationProvider> getAuthorizationProvider() {
    return Optional.empty();
  }

  @Override
  public Optional<KsqlUserContextProvider> getUserContextProvider() {
    return Optional.empty();
  }

  @Override
  public void close() {
  }
}
