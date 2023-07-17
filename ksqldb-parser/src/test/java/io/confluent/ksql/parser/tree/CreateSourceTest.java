/*
 * Copyright 2020 Confluent Inc.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Optional;
import org.junit.Test;

public class CreateSourceTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);

  private static final SourceName SOME_NAME = SourceName.of("bob");

  private static final CreateSourceProperties SOME_PROPS = CreateSourceProperties.from(
      ImmutableMap.of(
          "value_format", new StringLiteral("json"),
          "kafka_topic", new StringLiteral("foo")
      ));

  @Test
  public void shouldThrowOnMultipleKeyColumns() {
    // Given:
    final NodeLocation loc1 = new NodeLocation(2, 3);
    final ColumnName key1 = ColumnName.of("K1");

    final NodeLocation loc2 = new NodeLocation(4, 5);
    final ColumnName key2 = ColumnName.of("K2");

    final TableElements multipleKeys = TableElements.of(
        new TableElement(
            Optional.of(loc1),
            Namespace.PRIMARY_KEY,
            key1,
            new Type(SqlTypes.STRING)
        ),
        new TableElement(
            Optional.of(new NodeLocation(3, 4)),
            Namespace.VALUE,
            ColumnName.of("values are always valid"),
            new Type(SqlTypes.STRING)
        ),
        new TableElement(
            Optional.of(loc2),
            Namespace.KEY,
            key2,
            new Type(SqlTypes.STRING)
        )
    );

    // When:
    final ParseFailedException e = assertThrows(
        ParseFailedException.class,
        () -> new TestCreateSource(Optional.empty(), SOME_NAME, multipleKeys, false, false, SOME_PROPS)
    );

    // Then:
    assertThat(e.getUnloggedMessage(), containsString("Only single KEY column supported. "
        + "Multiple KEY columns found: `K1` (Line: 2, Col: 4), `K2` (Line: 4, Col: 6)"));
  }

  private static final class TestCreateSource extends CreateSource {

    TestCreateSource(
        final Optional<NodeLocation> location,
        final SourceName name,
        final TableElements elements,
        final boolean orReplace,
        final boolean notExists,
        final CreateSourceProperties properties
    ) {
      super(location, name, elements, orReplace, notExists, properties);
    }

    @Override
    public CreateSource copyWith(final TableElements elements,
        final CreateSourceProperties properties) {
      return null;
    }

    @Override
    public String toString() {
      return "";
    }
  }
}