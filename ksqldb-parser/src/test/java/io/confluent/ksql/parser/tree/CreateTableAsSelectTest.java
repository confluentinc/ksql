/*
 * Copyright 2019 Confluent Inc.
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
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Optional;
import org.junit.Test;

public class CreateTableAsSelectTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final SourceName SOME_NAME = SourceName.of("bob");
  private static final CreateSourceAsProperties SOME_PROPS = CreateSourceAsProperties.from(
      ImmutableMap.of("KAFKA_TOPIC", new StringLiteral("value"))
  );
  private static final Query SOME_QUERY = mock(Query.class);
  private static final TableElements SOME_ELEMENTS = TableElements.of(
      new TableElement(Namespace.VALUE, ColumnName.of("Bob"), new Type(SqlTypes.STRING))
  );

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new CreateTableAsSelect(SOME_NAME, SOME_QUERY, true, true, SOME_PROPS),
            new CreateTableAsSelect(Optional.of(SOME_LOCATION), SOME_NAME, SOME_QUERY, true, true, SOME_PROPS),
            new CreateTableAsSelect(Optional.of(SOME_LOCATION), SOME_NAME, SOME_QUERY, true, true, SOME_PROPS, Optional.empty())
        )
        .addEqualityGroup(
            new CreateTableAsSelect(SOME_NAME, SOME_QUERY, true, false, SOME_PROPS)
        )
        .addEqualityGroup(
            new CreateTableAsSelect(SourceName.of("diff"), SOME_QUERY, true, true, SOME_PROPS)
        )
        .addEqualityGroup(
            new CreateTableAsSelect(SOME_NAME, mock(Query.class), true, true, SOME_PROPS)
        )
        .addEqualityGroup(
            new CreateTableAsSelect(SOME_NAME, SOME_QUERY, false, true, SOME_PROPS)
        )
        .addEqualityGroup(
            new CreateTableAsSelect(SOME_NAME, SOME_QUERY, true, true, CreateSourceAsProperties.none())
        )
        .addEqualityGroup(
            new CreateStreamAsSelect(SOME_NAME, SOME_QUERY, true, true, CreateSourceAsProperties.none())
        )
        .addEqualityGroup(
            new CreateTableAsSelect(Optional.of(SOME_LOCATION), SOME_NAME, SOME_QUERY, false, true, SOME_PROPS, Optional.of(SOME_ELEMENTS))
        )
        .testEquals();
  }

  @Test
  public void shouldThrowOnNonePrimaryKey() {
    // Given:
    final NodeLocation loc = new NodeLocation(2, 3);
    final ColumnName name = ColumnName.of("K");

    final TableElements invalidElements = TableElements.of(
        new TableElement(
            Optional.of(loc),
            Namespace.KEY,
            name,
            new Type(SqlTypes.STRING)
        ),
        new TableElement(
            Optional.of(new NodeLocation(3, 4)),
            Namespace.VALUE,
            ColumnName.of("values are always valid"),
            new Type(SqlTypes.STRING)
        )
    );

    // When:
    final ParseFailedException e = assertThrows(
        ParseFailedException.class,
        () -> new CreateTableAsSelect(Optional.of(SOME_LOCATION), SOME_NAME, SOME_QUERY, false, false, SOME_PROPS, Optional.of(invalidElements))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Line: 2, Col: 4: Column `K` is a 'KEY' column: "
        + "please use 'PRIMARY KEY' for tables.\n"
        + "Tables have PRIMARY KEYs, which are unique and NON NULL.\n"
        + "Streams have KEYs, which have no uniqueness or NON NULL constraints."));
  }
}