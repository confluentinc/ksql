/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.ksql.properties;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class that validates if a property, or list of property overrides are part of allowed properties:
 * any property whose name is not in the configured allowlist is rejected (deny-by-default).
 * This is the inverse of {@link DenyListPropertyValidator} and the stricter of the two policies.
 *
 * <p>Notes on behavior:
 * <ul>
 *   <li><b>Exact-name matching only.</b></li>
 *   <li><b>No wildcards.</b> </li>
 *   <li><b>Empty allowlist is fail-closed.</b> </li>
 * </ul>
 */
public class AllowListPropertyValidator implements ConfigOverrideValidator {

  /** Server-owned keys that must never be permitted as overrides, even if allowlisted. */
  private static final Set<String> ALWAYS_DENIED = ImmutableSet.of(
      KsqlConfig.KSQL_SERVICE_ID_CONFIG,
      KsqlConfig.KSQL_PROPERTIES_OVERRIDES_DENYLIST,
      KsqlConfig.KSQL_PROPERTIES_OVERRIDES_ALLOWLIST,
      KsqlConfig.KSQL_PROPERTIES_OVERRIDES_VALIDATION_MODE,
      KsqlConfig.KSQL_PROPERTIES_OVERRIDES_LOG
  );

  private final Set<String> allowedProps;

  public AllowListPropertyValidator(final Collection<String> allowlist) {
    Objects.requireNonNull(allowlist, "allowlist");
    rejectWildcards(allowlist);
    this.allowedProps = Sets.difference(ImmutableSet.copyOf(allowlist), ALWAYS_DENIED)
                        .immutableCopy();
  }

  /**
   * Validates that every property override is present in the allowlist.
   * @throws KsqlException if at least one property is not on the allowlist.
   */
  @Override
  public void validateAll(final Map<String, Object> properties) {
    final Set<String> notAllowedProps = Sets.difference(properties.keySet(), allowedProps);
    if (!notAllowedProps.isEmpty()) {
      throw new KsqlException(String.format("One or more property overrides set locally are "
          + "not permitted by the KSQL server allowlist (use UNSET to reset their default "
          + "value): %s", notAllowedProps));
    }
  }

  private static void rejectWildcards(final Collection<String> allowlist) {
    final List<String> withWildcards = allowlist.stream()
        .filter(name -> name.indexOf('*') >= 0 || name.indexOf('?') >= 0)
        .collect(Collectors.toList());
    if (!withWildcards.isEmpty()) {
      throw new KsqlException("Allowlist entries must be exact property names; wildcard or glob "
          + "entries are not permitted: " + withWildcards);
    }
  }
}
