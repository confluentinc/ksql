/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class UnsetProperty extends Statement implements ExecutableDdlStatement {

  private final String propertyName;

  public UnsetProperty(final Optional<NodeLocation> location, final String propertyName) {
    super(location);
    this.propertyName = requireNonNull(propertyName, "propertyName");
  }

  public String getPropertyName() {
    return propertyName;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UnsetProperty)) {
      return false;
    }
    final UnsetProperty that = (UnsetProperty) o;
    return Objects.equals(propertyName, that.propertyName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(propertyName);
  }

  @Override
  public String toString() {
    return "UnsetProperty{"
        + "propertyName='" + propertyName + '\''
        + '}';
  }
}
