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
 * Validates property overrides against an allowlist: any property whose name is not in the
 * configured allowlist is rejected (deny-by-default). This is the inverse of
 * {@link DenyListPropertyValidator} and the stricter of the two policies.
 *
 * <p>Notes on behavior:
 * <ul>
 *   <li><b>Exact-name matching only.</b> Membership is tested against the raw property name, the
 *       same way the denylist works today. Prefixed variants (e.g. {@code auto.offset.reset} vs.
 *       {@code ksql.streams.auto.offset.reset}) are distinct entries. Canonicalization is tracked
 *       separately and not done here.</li>
 *   <li><b>No wildcards.</b> Allowlist entries must be exact property names; entries containing
 *       glob/regex meta-characters ({@code *} or {@code ?}) are rejected at construction time so a
 *       broad pattern cannot silently re-open the "allow arbitrary prefix" hole.</li>
 *   <li><b>Always-denied floor.</b> A small set of server-owned keys (the service id and the
 *       {@code ksql.properties.overrides.*} policy knobs) is removed from the effective allowlist
 *       so they can never be permitted, even if listed by mistake. Mirrors how the denylist
 *       force-adds the service id.</li>
 *   <li><b>Empty allowlist is fail-closed.</b> An empty allowlist rejects every override.</li>
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

  private final Set<String> allowed;

  public AllowListPropertyValidator(final Collection<String> allowlist) {
    Objects.requireNonNull(allowlist, "allowlist");
    rejectWildcards(allowlist);
    this.allowed = Sets.difference(ImmutableSet.copyOf(allowlist), ALWAYS_DENIED).immutableCopy();
  }

  /**
   * Validates that every property override is present in the allowlist.
   * @throws KsqlException if at least one property is not on the allowlist.
   */
  @Override
  public void validateAll(final Map<String, Object> properties) {
    final Set<String> notAllowed = Sets.difference(properties.keySet(), allowed);
    if (!notAllowed.isEmpty()) {
      throw new KsqlException(String.format("One or more property overrides set locally are "
          + "not permitted by the KSQL server allowlist (use UNSET to reset their default "
          + "value): %s", notAllowed));
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
