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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import java.util.Optional;

@Immutable
public class CreateTableAsSelect extends CreateAsSelect {

  public CreateTableAsSelect(
      final SourceName name,
      final Query query,
      final boolean notExists,
      final boolean orReplace,
      final CreateSourceAsProperties properties
  ) {
    this(Optional.empty(), name, query, notExists, orReplace, properties);
  }

  public CreateTableAsSelect(
      final Optional<NodeLocation> location,
      final SourceName name,
      final Query query,
      final boolean notExists,
      final boolean orReplace,
      final CreateSourceAsProperties properties
  ) {
    this(location, name, query, orReplace, notExists, properties, Optional.empty());
  }

  public CreateTableAsSelect(
      final Optional<NodeLocation> location,
      final SourceName name,
      final Query query,
      final boolean notExists,
      final boolean orReplace,
      final CreateSourceAsProperties properties,
      final Optional<TableElements> elements
  ) {
    super(location, name, query, orReplace, notExists, properties, elements);
    throwOnNonPrimaryKeys(getElements());
  }

  private CreateTableAsSelect(
      final CreateTableAsSelect other,
      final CreateSourceAsProperties properties
  ) {
    super(other, properties);
  }

  @Override
  public CreateAsSelect copyWith(final CreateSourceAsProperties properties) {
    return new CreateTableAsSelect(this, properties);
  }

  @Override
  public CreateAsSelect copyWith(
      final TableElements elements,
      final CreateSourceAsProperties properties
  ) {
    return new CreateTableAsSelect(
        getLocation(),
        getName(),
        getQuery(),
        isNotExists(),
        isOrReplace(),
        properties,
        Optional.ofNullable(elements)
    );
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitCreateTableAsSelect(this, context);
  }

  @Override
  public String toString() {
    return "CreateTableAsSelect{" + super.toString() + '}';
  }

  private static void throwOnNonPrimaryKeys(final Optional<TableElements> optionalElements) {
    optionalElements.ifPresent(elements -> {
      final Optional<TableElement> wrongKey = elements.stream()
          .filter(e -> e.getNamespace().isKey() && e.getNamespace() != Namespace.PRIMARY_KEY)
          .findFirst();

      wrongKey.ifPresent(col -> {
        final String loc = NodeLocation.asPrefix(col.getLocation());
        throw new ParseFailedException(
            loc + "Column " + col.getName() + " is a 'KEY' column: "
                + "please use 'PRIMARY KEY' for tables."
                + System.lineSeparator()
                + "Tables have PRIMARY KEYs, which are unique and NON NULL."
                + System.lineSeparator()
                + "Streams have KEYs, which have no uniqueness or NON NULL constraints."
        );
      });
    });
  }
}
