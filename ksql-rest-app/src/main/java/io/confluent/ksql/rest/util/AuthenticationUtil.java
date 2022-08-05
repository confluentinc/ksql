/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.rest.util;

import io.confluent.ksql.security.KsqlAuthTokenProvider;
import io.confluent.ksql.util.KsqlConfig;
import java.security.Principal;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthenticationUtil {

  final Clock clock;
  private static final Logger log = LoggerFactory.getLogger(AuthenticationUtil.class);

  public AuthenticationUtil(final Clock clock) {
    this.clock = Objects.requireNonNull(clock);
  }

  public Optional<Long> getTokenTimeout(
      final Principal principal,
      final KsqlConfig ksqlConfig,
      final Optional<KsqlAuthTokenProvider> authTokenProvider
  ) {
    final long maxTimeout =
        ksqlConfig.getLong(KsqlConfig.KSQL_WEBSOCKET_CONNECTION_MAX_TIMEOUT_MS);
    if (maxTimeout > 0) {
      if (authTokenProvider.isPresent()) {
        try {
          final long tokenTimeout = Math.max(authTokenProvider.get().getLifetimeMs(principal)
              - clock.millis(), 0);
          return Optional.of(Math.min(tokenTimeout, maxTimeout));
        } catch (final Exception e) {
          log.error(e.getMessage());
        }
      }
      return Optional.of(maxTimeout);
    } else {
      return Optional.empty();
    }
  }

}