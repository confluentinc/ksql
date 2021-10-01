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

package io.confluent.ksql.serde.connect;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.util.CompatibleSet;
import java.util.Set;

/**
 * Validated set of connect schema translation policies
 */
public class ConnectSchemaTranslationPolicies extends
    CompatibleSet<ConnectSchemaTranslationPolicy> {

  public static ConnectSchemaTranslationPolicies from(
      final Set<ConnectSchemaTranslationPolicy> features) {
    return new ConnectSchemaTranslationPolicies(features);
  }

  public static ConnectSchemaTranslationPolicies of(
      final ConnectSchemaTranslationPolicy... features) {
    return new ConnectSchemaTranslationPolicies(ImmutableSet.copyOf(features));
  }

  private ConnectSchemaTranslationPolicies(Set<ConnectSchemaTranslationPolicy> policies) {
    super(policies);
  }

  public boolean enabled(ConnectSchemaTranslationPolicy policy) {
    return contains(policy);
  }
}
