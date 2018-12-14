/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.Optional;

public class SetProperty extends Statement implements DdlStatement {

  private final String propertyName;
  private final String propertyValue;


  public SetProperty(final Optional<NodeLocation> location, final String propertyName,
                      final String propertyValue) {
    super(location);
    requireNonNull(propertyName, "propertyName is null");
    requireNonNull(propertyValue, "propertyValue is null");
    this.propertyName = propertyName;
    this.propertyValue = propertyValue;
  }

  public String getPropertyName() {
    return propertyName;
  }

  public String getPropertyValue() {
    return propertyValue;
  }

  @Override
  public int hashCode() {
    return Objects.hash(propertyName, propertyValue);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final SetProperty setProperty = (SetProperty) o;

    if (!propertyName.equals(setProperty.propertyName)) {
      return false;
    }
    if (!propertyValue.equals(setProperty.propertyValue)) {
      return false;
    }

    return true;
  }

  @Override
  public String toString() {
    return toStringHelper(this).toString();
  }
}
