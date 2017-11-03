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

public class AllColumns
    extends SelectItem {

  private final QualifiedName prefix;

  public AllColumns() {
    this(null);
  }

  public AllColumns(QualifiedName prefix) {
    this.prefix = prefix;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAllColumns(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AllColumns that = (AllColumns) o;
    return Objects.equals(prefix, that.prefix);
  }

  @Override
  public int hashCode() {
    return prefix.hashCode();
  }

  @Override
  public String toString() {
    if (prefix != null) {
      return prefix + ".*";
    }

    return "*";
  }
}
