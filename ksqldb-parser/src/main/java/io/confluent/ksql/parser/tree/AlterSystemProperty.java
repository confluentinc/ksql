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

package io.confluent.ksql.parser.tree;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class AlterSystemProperty extends Statement {

  private final String propertyName;
  private final String propertyValue;

  public AlterSystemProperty(
      final Optional<NodeLocation> location,
      final String propertyName,
      final String propertyValue
  ) {
    super(location);
    this.propertyName = requireNonNull(propertyName, "propertyName");
    this.propertyValue = requireNonNull(propertyValue, "propertyValue");
  }

  public String getPropertyName() {
    return propertyName;
  }

  public String getPropertyValue() {
    return propertyValue;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitAlterSystemProperty(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AlterSystemProperty that = (AlterSystemProperty) o;
    return Objects.equals(propertyName, that.propertyName)
        && Objects.equals(propertyValue, that.propertyValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(propertyName, propertyValue);
  }

  @Override
  public String toString() {
    return "AlterSystemProperty{"
        + "propertyName='" + propertyName + '\''
        + ", propertyValue='" + propertyValue + '\''
        + '}';
  }
}
