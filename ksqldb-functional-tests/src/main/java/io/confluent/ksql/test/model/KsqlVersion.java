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

package io.confluent.ksql.test.model;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.model.SemanticVersion;
import io.confluent.ksql.testing.EffectivelyImmutable;
import io.confluent.ksql.util.AppInfo;
import java.util.Comparator;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Immutable
public final class KsqlVersion implements Comparable<KsqlVersion> {

  private static final Pattern VERSION_PATTERN = Pattern
      .compile("(?<major>\\d+)\\.(?<minor>\\d+)(?<patch>.\\d+)?(?:-(SNAPSHOT|\\d+))?");

  @EffectivelyImmutable
  private static final Comparator<KsqlVersion> COMPARATOR =
      Comparator.comparing(KsqlVersion::getVersion)
      .thenComparingLong(v -> v.timestamp);

  private final transient String name;
  private final SemanticVersion version;
  private final long timestamp;

  public static KsqlVersion current() {
    return parse(AppInfo.getVersion());
  }

  public static KsqlVersion of(final String name, final SemanticVersion version) {
    return new KsqlVersion(name, version, Long.MAX_VALUE);
  }

  public static KsqlVersion parse(final String version) {
    final Matcher matcher = VERSION_PATTERN.matcher(version);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          "Failed to parse version: '" + version + "'. "
              + "Version must be in format <major>.<minor>[.<patch>][-SNAPSHOT]"
      );
    }

    final int major = Integer.parseInt(matcher.group("major"));
    final int minor = Integer.parseInt(matcher.group("minor"));
    final int patch = matcher.group("patch") == null
        ? 0
        : Integer.parseInt(matcher.group("patch").substring(1));

    final SemanticVersion v = SemanticVersion.of(major, minor, patch);
    return new KsqlVersion(version, v, Long.MAX_VALUE);
  }

  public KsqlVersion withTimestamp(final long timestamp) {
    return new KsqlVersion(name, version, timestamp);
  }

  public String getName() {
    return name;
  }

  public SemanticVersion getVersion() {
    return version;
  }

  public OptionalLong getTimestamp() {
    return timestamp == Long.MAX_VALUE ? OptionalLong.empty() : OptionalLong.of(timestamp);
  }

  @Override
  public int compareTo(final KsqlVersion other) {
    return COMPARATOR.compare(this, other);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KsqlVersion that = (KsqlVersion) o;
    return Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version);
  }

  @Override
  public String toString() {
    return name + " (" + version + ")";
  }

  private KsqlVersion(final String name, final SemanticVersion version, final long timestamp) {
    this.name = Objects.requireNonNull(name, "name");
    this.version = Objects.requireNonNull(version, "version");
    this.timestamp = timestamp;
  }
}
