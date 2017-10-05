/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class UnsetProperty extends Statement {

  private final String propertyName;

  public UnsetProperty(Optional<NodeLocation> location, String propertyName) {
    super(location);
    requireNonNull(propertyName, "propertyName is null");
    this.propertyName = propertyName;
  }

  public String getPropertyName() {
    return propertyName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UnsetProperty)) {
      return false;
    }
    UnsetProperty that = (UnsetProperty) o;
    return Objects.equals(getPropertyName(), that.getPropertyName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getPropertyName());
  }

  @Override
  public String toString() {
    return toStringHelper(this).toString();
  }
}
