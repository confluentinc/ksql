/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import java.util.Objects;
import java.util.Set;

@Immutable
public final class Formats {
  private static final String KEY_FORMAT = "keyFormat";
  private static final String VALUE_FORMAT = "valueFormat";
  private static final String SERDE_OPTIONS = "serdeOptions";

  @JsonProperty(KEY_FORMAT)
  private final KeyFormat keyFormat;
  @JsonProperty(VALUE_FORMAT)
  private final ValueFormat valueFormat;
  @JsonProperty(SERDE_OPTIONS)
  private final Set<SerdeOption> options;

  public static Formats of(
      final KeyFormat keyFormat,
      final ValueFormat valueFormat,
      final Set<SerdeOption> options) {
    return new Formats(keyFormat, valueFormat, options);
  }

  @JsonCreator
  private Formats(
      @JsonProperty(KEY_FORMAT) final KeyFormat keyFormat,
      @JsonProperty(VALUE_FORMAT) final ValueFormat valueFormat,
      @JsonProperty(SERDE_OPTIONS) final Set<SerdeOption> options) {
    this.keyFormat = Objects.requireNonNull(keyFormat, "keyFormat");
    this.valueFormat = Objects.requireNonNull(valueFormat, "valueFormat");
    this.options = Objects.requireNonNull(options, "options");
  }

  public KeyFormat getKeyFormat() {
    return keyFormat;
  }

  public ValueFormat getValueFormat() {
    return valueFormat;
  }

  public Set<SerdeOption> getOptions() {
    return options;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Formats serdeInfo = (Formats) o;
    return Objects.equals(keyFormat, serdeInfo.keyFormat)
        && Objects.equals(valueFormat, serdeInfo.valueFormat)
        && Objects.equals(options, serdeInfo.options);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyFormat, valueFormat, options);
  }
}
