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

package io.confluent.ksql.test.tools;

import com.google.common.collect.Range;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.test.model.KsqlVersion;
import java.util.Optional;

/**
 * Pojo for holding the bounds for which a test is valid
 */
@Immutable
public final class VersionBounds {

  private final Range<KsqlVersion> range;

  public static VersionBounds of(
      final Optional<KsqlVersion> min,
      final Optional<KsqlVersion> max
  ) {
    return new VersionBounds(min, max);
  }

  public static VersionBounds allVersions() {
    return of(Optional.empty(), Optional.empty());
  }

  private VersionBounds(
      final Optional<KsqlVersion> min,
      final Optional<KsqlVersion> max
  ) {
    if (min.isPresent() && max.isPresent()) {
      this.range = Range.closed(min.get(), max.get());
    } else if (!min.isPresent() && !max.isPresent()) {
      this.range = Range.all();
    } else {
      this.range = min
          .map(Range::atLeast)
          .orElseGet(() -> Range.atMost(max.get()));
    }
  }

  public boolean contains(final KsqlVersion version) {
    return range.contains(version);
  }

  @Override
  public String toString() {
    return range.toString();
  }
}
