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

import static org.mockito.Mockito.mock;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import java.util.Optional;
import org.junit.Test;

public class InsertIntoTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final QualifiedName SOME_NAME = QualifiedName.of("bob");
  private static final Query SOME_QUERY = mock(Query.class);
  private static final Optional<Expression> SOME_COLUMN = Optional.of(mock(Expression.class));

  @Test
  public void shouldThrowNpeOnConstruction() {
    new NullPointerTester()
        .setDefault(NodeLocation.class, SOME_LOCATION)
        .setDefault(Query.class, SOME_QUERY)
        .setDefault(QualifiedName.class, SOME_NAME)
        .testAllPublicConstructors(InsertInto.class);
  }

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new InsertInto(SOME_NAME, SOME_QUERY, SOME_COLUMN),
            new InsertInto(SOME_NAME, SOME_QUERY, SOME_COLUMN),
            new InsertInto(Optional.of(SOME_LOCATION), SOME_NAME, SOME_QUERY, SOME_COLUMN),
            new InsertInto(Optional.of(OTHER_LOCATION), SOME_NAME, SOME_QUERY, SOME_COLUMN)
        )
        .addEqualityGroup(
            new InsertInto(QualifiedName.of("diff"), SOME_QUERY, SOME_COLUMN)
        )
        .addEqualityGroup(
            new InsertInto(SOME_NAME, mock(Query.class), SOME_COLUMN)
        )
        .addEqualityGroup(
            new InsertInto(SOME_NAME, SOME_QUERY, Optional.empty())
        )
        .testEquals();
  }
}