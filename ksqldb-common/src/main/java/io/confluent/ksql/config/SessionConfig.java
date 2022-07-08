/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.config;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.testing.EffectivelyImmutable;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Objects;

/**
 * Combination of the system config + session overrides provided by the user.
 */
@Immutable
public final class SessionConfig {

  @EffectivelyImmutable // The map stores primitive types only.
  private final ImmutableMap<String, Object> overrides;
  private final KsqlConfig systemConfig;

  /**
   * Create an instance.
   *
   * @param systemConfig the system config the server started up with.
   * @param overrides    the overrides that came as part of the users request.
   * @return the session config
   */
  public static SessionConfig of(final KsqlConfig systemConfig, final Map<String, ?> overrides) {
    return new SessionConfig(systemConfig, overrides);
  }

  /**
   * Create a new sessionConfig with additional overrides
   *
   * @param additionOverrides additional overrides to pass
   * @return the session config
   */
  public SessionConfig copyWith(final Map<String, ?> additionOverrides) {
    final ImmutableMap<String, Object> newOverrides = ImmutableMap.<String, Object>builder()
        .putAll(overrides)
        .putAll(additionOverrides).build();

    return new SessionConfig(systemConfig, newOverrides);
  }

  /**
   * Create a new sessionConfig with new overrides
   *
   * @param newOverrides additional overrides to pass
   * @return the session config
   */
  public SessionConfig withNewOverrides(final Map<String, ?> newOverrides) {
    return new SessionConfig(systemConfig, newOverrides);
  }

  private SessionConfig(final KsqlConfig systemConfig, final Map<String, ?> overrides) {
    this.systemConfig = Objects.requireNonNull(systemConfig, "systemConfig");
    this.overrides = ImmutableMap.copyOf(Objects.requireNonNull(overrides, "overrides"));
  }

  /**
   * Get the `KsqlConfig` instance, either with or without the overrides applied.
   *
   * <p>Calling with {@code withOverridesApplied} of {@code true} will return the system config
   * with the user's overrides applied. This is the more common case. This can be useful when the
   * user should be able to influence how code executes by using `SET` to provide config overrides.
   *
   * <p>Calling with {@code withOverridesApplied} of {@code false} will return the system config
   * without any overrides. This can be useful if the users shouldn't be able to supply overrides to
   * change the behaviour of the code you're passing the config to.
   *
   * @param withOverridesApplied flag to indicate if the user supplied overrides should be applied.
   * @return the config.
   */
  public KsqlConfig getConfig(final boolean withOverridesApplied) {
    return withOverridesApplied
        ? systemConfig.cloneWithPropertyOverwrite(overrides)
        : systemConfig;
  }

  /**
   * @return User supplied overrides, i.e. those provided in the request to the server.
   */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "overrides is ImmutableMap")
  public Map<String, Object> getOverrides() {
    return overrides;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SessionConfig that = (SessionConfig) o;
    return Objects.equals(overrides, that.overrides)
        && Objects.equals(systemConfig, that.systemConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(overrides, systemConfig);
  }

  @Override
  public String toString() {
    return "SessionConfig{"
        + "overrides=" + overrides
        + ", systemConfig=" + systemConfig
        + '}';
  }
}
