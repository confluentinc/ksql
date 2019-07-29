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

import static com.google.common.base.MoreObjects.toStringHelper;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import java.util.Optional;

@Immutable
public class CreateTable extends CreateSource implements ExecutableDdlStatement {

  public CreateTable(
      final QualifiedName name,
      final TableElements elements,
      final boolean notExists,
      final CreateSourceProperties properties
  ) {
    this(Optional.empty(), name, elements, notExists, properties);
  }

  public CreateTable(
      final Optional<NodeLocation> location,
      final QualifiedName name,
      final TableElements elements,
      final boolean notExists,
      final CreateSourceProperties properties
  ) {
    super(location, name, elements, notExists, properties);
  }

  @Override
  public CreateSource copyWith(
      final TableElements elements,
      final CreateSourceProperties properties
  ) {
    return new CreateTable(
        getLocation(),
        getName(),
        elements,
        isNotExists(),
        properties);
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitCreateTable(this, context);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    return super.equals(obj);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", getName())
        .add("elements", getElements())
        .add("notExists", isNotExists())
        .add("properties", getProperties())
        .toString();
  }
}
