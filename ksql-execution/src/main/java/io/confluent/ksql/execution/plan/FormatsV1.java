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
public final class FormatsV1 implements Formats {
  private final KeyFormat keyFormat;
  private final ValueFormat valueFormat;
  private final Set<SerdeOption> options;

  @JsonCreator
  FormatsV1(
      @JsonProperty(value = "keyFormat", required = true) final KeyFormat keyFormat,
      @JsonProperty(value = "valueFormat", required = true) final ValueFormat valueFormat,
      @JsonProperty(value = "options", required = true) final Set<SerdeOption> options) {
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
    final FormatsV1 serdeInfo = (FormatsV1) o;
    return Objects.equals(keyFormat, serdeInfo.keyFormat)
        && Objects.equals(valueFormat, serdeInfo.valueFormat)
        && Objects.equals(options, serdeInfo.options);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyFormat, valueFormat, options);
  }
}
