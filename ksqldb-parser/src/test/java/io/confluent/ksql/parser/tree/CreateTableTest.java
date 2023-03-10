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

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Optional;
import org.junit.Test;

public class CreateTableTest {

  private static final ColumnConstraints KEY_CONSTRAINT =
      new ColumnConstraints.Builder().key().build();

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final SourceName SOME_NAME = SourceName.of("bob");
  private static final TableElements SOME_ELEMENTS = TableElements.of(
      new TableElement(ColumnName.of("Bob"), new Type(SqlTypes.STRING))
  );
  private static final CreateSourceProperties SOME_PROPS = CreateSourceProperties.from(
      ImmutableMap.of(
      "value_format", new StringLiteral("json"),
      "kafka_topic", new StringLiteral("foo")
      ));
  private static final CreateSourceProperties OTHER_PROPS = CreateSourceProperties.from(
      ImmutableMap.of(
          "value_format", new StringLiteral("json"),
          "kafka_topic", new StringLiteral("foo"),
          CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY, new StringLiteral("foo"))
  );

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new CreateTable(SOME_NAME, SOME_ELEMENTS, false, true, SOME_PROPS, false),
            new CreateTable(SOME_NAME, SOME_ELEMENTS, false, true, SOME_PROPS, false),
            new CreateTable(Optional.of(SOME_LOCATION), SOME_NAME, SOME_ELEMENTS, false, true, SOME_PROPS, false),
            new CreateTable(Optional.of(OTHER_LOCATION), SOME_NAME, SOME_ELEMENTS, false, true, SOME_PROPS, false)
        )
        .addEqualityGroup(
            new CreateTable(SourceName.of("jim"), SOME_ELEMENTS, false, true, SOME_PROPS, false)
        )
        .addEqualityGroup(
            new CreateTable(SOME_NAME, TableElements.of(), false, true, SOME_PROPS, false)
        )
        .addEqualityGroup(
            new CreateTable(SOME_NAME, SOME_ELEMENTS, false, false, SOME_PROPS, false)
        )
        .addEqualityGroup(
            new CreateTable(SOME_NAME, SOME_ELEMENTS, true, true, SOME_PROPS, true)
        )
        .addEqualityGroup(
            new CreateTable(SOME_NAME, SOME_ELEMENTS, false, true, OTHER_PROPS, false)
        )
        .addEqualityGroup(
            new CreateTable(SOME_NAME, SOME_ELEMENTS, false, false, OTHER_PROPS, true)
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
            name,
            new Type(SqlTypes.STRING),
            KEY_CONSTRAINT
        ),
        new TableElement(
            Optional.of(new NodeLocation(3, 4)),
            ColumnName.of("values are always valid"),
            new Type(SqlTypes.STRING),
            ColumnConstraints.NO_COLUMN_CONSTRAINTS
        )
    );

    // When:
    final ParseFailedException e = assertThrows(
        ParseFailedException.class,
        () -> new CreateTable(SOME_NAME, invalidElements, false, false, SOME_PROPS, false)
    );

    // Then:
    assertThat(e.getUnloggedMessage(), containsString(
        "Line: 2, Col: 4: Column `K` is a 'KEY' column: "
        + "please use 'PRIMARY KEY' for tables.\n"
        + "Tables have PRIMARY KEYs, which are unique and NON NULL.\n"
        + "Streams have KEYs, which have no uniqueness or NON NULL constraints."));
    assertThat(e.getMessage(), containsString("Line: 2, Col: 4: Column is a 'KEY' column: "
        + "please use 'PRIMARY KEY' for tables.\n"
        + "Tables have PRIMARY KEYs, which are unique and NON NULL.\n"
        + "Streams have KEYs, which have no uniqueness or NON NULL constraints."));
    assertThat(e.getSqlStatement(), containsString("K"));
  }
}