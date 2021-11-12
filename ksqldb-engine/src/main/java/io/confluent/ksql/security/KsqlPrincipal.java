/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.security;

import java.security.Principal;
import java.util.Map;

public interface KsqlPrincipal extends Principal {

  Map<String, Object> getUserProperties();

  static KsqlPrincipal from(final Principal principal) {
    return principal instanceof KsqlPrincipal
        ? (KsqlPrincipal) principal
        : new DefaultKsqlPrincipal(principal);
  }
}
