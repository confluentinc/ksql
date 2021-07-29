/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.plan;

import static io.confluent.ksql.execution.plan.JoinType.INNER;
import static io.confluent.ksql.execution.plan.JoinType.LEFT;
import static io.confluent.ksql.execution.plan.JoinType.OUTER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.Cast;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ForeignKeyTableTableJoinTest {

  private static final Optional<ColumnName> JOIN_COLUMN_NAME = Optional.of(ColumnName.of("Bob"));
  private static final Optional<ColumnName> JOIN_COLUMN_NAME_2 = Optional.of(ColumnName.of("Vic"));
  private static final Optional<Expression> JOIN_EXPRESSION = Optional.of(
      new Cast(
        new UnqualifiedColumnReferenceExp(Optional.empty(), ColumnName.of("Bob")),
        new Type(SqlPrimitiveType.of("INT"))
      ));
  private static final Optional<Expression> JOIN_EXPRESSION_2 = Optional.of(
      new Cast(
        new UnqualifiedColumnReferenceExp(Optional.empty(), ColumnName.of("Vic")),
      new Type(SqlPrimitiveType.of("VARCHAR"))
      ));

  @Mock
  private ExecutionStepPropertiesV1 props1;
  @Mock
  private ExecutionStepPropertiesV1 props2;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> left1;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> right1;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> left2;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> right2;
  @Mock
  private Formats formats1;
  @Mock
  private Formats formats2;

  @Test
  public void shouldNotAllowOuterJoin() {
    final IllegalArgumentException error = assertThrows(
        IllegalArgumentException.class,
        () -> new ForeignKeyTableTableJoin<>(props1, OUTER, JOIN_COLUMN_NAME, Optional.empty(), formats1, left1, right1)
    );

    assertThat(error.getMessage(), is("OUTER join not supported."));
  }

  @Test
  public void shouldNotAllowEmptyColumnNameAndEmptyExpression() {
    final IllegalArgumentException error = assertThrows(
        IllegalArgumentException.class,
        () -> new ForeignKeyTableTableJoin<>(props1, INNER, Optional.empty(), Optional.empty(), formats1, left1, right1)
    );

    assertThat(error.getMessage(), is("Either leftJoinColumnName or leftJoinExpression must be provided."));
  }

  @Test
  public void shouldNotAllowColumnNameAndExpression() {
    final IllegalArgumentException error = assertThrows(
        IllegalArgumentException.class,
        () -> new ForeignKeyTableTableJoin<>(props1, INNER, JOIN_COLUMN_NAME, JOIN_EXPRESSION, formats1, left1, right1)
    );

    assertThat(error.getMessage(), is("Either leftJoinColumnName or leftJoinExpression must be empty."));
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementEqualsColumName() {
    new EqualsTester()
        .addEqualityGroup(
            new ForeignKeyTableTableJoin<>(props1, INNER, JOIN_COLUMN_NAME, Optional.empty(), formats1, left1, right1),
            new ForeignKeyTableTableJoin<>(props1, INNER, JOIN_COLUMN_NAME, Optional.empty(), formats1, left1, right1)
        )
        .addEqualityGroup(
            new ForeignKeyTableTableJoin<>(props2, INNER, JOIN_COLUMN_NAME, Optional.empty(), formats1, left1, right1)
        )
        .addEqualityGroup(
            new ForeignKeyTableTableJoin<>(props1, LEFT, JOIN_COLUMN_NAME, Optional.empty(), formats1, left1, right1)
        )
        .addEqualityGroup(
            new ForeignKeyTableTableJoin<>(props1, INNER, JOIN_COLUMN_NAME_2, Optional.empty(), formats1, left1, right1)
        )
        .addEqualityGroup(
            new ForeignKeyTableTableJoin<>(props1, INNER, JOIN_COLUMN_NAME, Optional.empty(), formats2, left1, right1)
        )
        .addEqualityGroup(
            new ForeignKeyTableTableJoin<>(props1, INNER, JOIN_COLUMN_NAME, Optional.empty(), formats1, left2, right1)
        )
        .addEqualityGroup(
            new ForeignKeyTableTableJoin<>(props1, INNER, JOIN_COLUMN_NAME, Optional.empty(), formats1, left1, right2)
        )
        .testEquals();
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementEqualsExpression() {
    new EqualsTester()
        .addEqualityGroup(
            new ForeignKeyTableTableJoin<>(props1, INNER, Optional.empty(), JOIN_EXPRESSION, formats1, left1, right1),
            new ForeignKeyTableTableJoin<>(props1, INNER, Optional.empty(), JOIN_EXPRESSION, formats1, left1, right1)
        )
        .addEqualityGroup(
            new ForeignKeyTableTableJoin<>(props2, INNER, Optional.empty(), JOIN_EXPRESSION, formats1, left1, right1)
        )
        .addEqualityGroup(
            new ForeignKeyTableTableJoin<>(props1, LEFT, Optional.empty(), JOIN_EXPRESSION, formats1, left1, right1)
        )
        .addEqualityGroup(
            new ForeignKeyTableTableJoin<>(props1, INNER, Optional.empty(), JOIN_EXPRESSION_2, formats1, left1, right1)
        )
        .addEqualityGroup(
            new ForeignKeyTableTableJoin<>(props1, INNER, Optional.empty(), JOIN_EXPRESSION, formats2, left1, right1)
        )
        .addEqualityGroup(
            new ForeignKeyTableTableJoin<>(props1, INNER, Optional.empty(), JOIN_EXPRESSION, formats1, left2, right1)
        )
        .addEqualityGroup(
            new ForeignKeyTableTableJoin<>(props1, INNER, Optional.empty(), JOIN_EXPRESSION, formats1, left1, right2)
        )
        .testEquals();
  }
}
