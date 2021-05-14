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

package io.confluent.ksql.model;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.Comparator;
import java.util.Objects;

@Immutable
public final class SemanticVersion implements Comparable<SemanticVersion> {

  @EffectivelyImmutable
  private static final Comparator<SemanticVersion> COMPARATOR =
      Comparator.comparingInt(SemanticVersion::major)
      .thenComparingInt(SemanticVersion::minor)
      .thenComparingInt(SemanticVersion::patch);

  private final int major;
  private final int minor;
  private final int patch;

  public static SemanticVersion of(final int major, final int minor, final int patch) {
    return new SemanticVersion(major, minor, patch);
  }

  private SemanticVersion(final int major, final int minor, final int patch) {
    this.major = major;
    this.minor = minor;
    this.patch = patch;
  }

  public int major() {
    return major;
  }

  public int minor() {
    return minor;
  }

  public int patch() {
    return patch;
  }

  @Override
  public int compareTo(final SemanticVersion other) {
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
    final SemanticVersion that = (SemanticVersion) o;
    return major == that.major
        && minor == that.minor
        && patch == that.patch;
  }

  @Override
  public int hashCode() {
    return Objects.hash(major, minor, patch);
  }

  @Override
  public String toString() {
    return major + "." + minor + "." + patch;
  }
}
