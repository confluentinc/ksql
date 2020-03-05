/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.server;

import java.security.Principal;
import java.util.Optional;

public class DummyApiSecurityContext implements ApiSecurityContext {

  @Override
  public Principal getPrincipal() {
    return new DummyPrincipal();
  }

  @Override
  public Optional<String> getAuthToken() {
    return Optional.empty();
  }

  private static class DummyPrincipal implements Principal {

    @Override
    public String getName() {
      return "no_principal";
    }
  }
}
