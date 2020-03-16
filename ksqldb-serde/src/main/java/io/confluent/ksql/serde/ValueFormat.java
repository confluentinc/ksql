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

package io.confluent.ksql.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.errorprone.annotations.Immutable;
import java.util.Objects;

/**
 * Immutable Pojo holding information about a source's value format.
 */
@Immutable
public final class ValueFormat {

  private final FormatInfo format;

  @JsonCreator
  public static ValueFormat of(
      final FormatInfo format
  ) {
    return new ValueFormat(format);
  }

  private ValueFormat(final FormatInfo format) {
    this.format = Objects.requireNonNull(format, "format");
  }

  @JsonIgnore
  public Format getFormat() {
    return FormatFactory.of(format);
  }

  @JsonValue
  public FormatInfo getFormatInfo() {
    return format;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ValueFormat that = (ValueFormat) o;
    return Objects.equals(format, that.format);
  }

  @Override
  public int hashCode() {
    return Objects.hash(format);
  }

  @Override
  public String toString() {
    return "ValueFormat{"
        + "format=" + format
        + '}';
  }
}
