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

package io.confluent.ksql.engine.rewrite;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LambdaFunctionCall;
import io.confluent.ksql.execution.expression.tree.LambdaVariable;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.AstBuilder;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AstSanitizerTest {

  private static final MetaStore META_STORE = MetaStoreFixture
      .getNewMetaStore(mock(FunctionRegistry.class));

  private static final SourceName TEST1_NAME = SourceName.of("TEST1");

  @Test
  public void shouldThrowIfInsertIntoSourceWithHeader() {
    // Given:
    final Statement stmt = givenQuery("INSERT INTO TEST1 SELECT * FROM TEST0;");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> AstSanitizer.sanitize(stmt, META_STORE)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Cannot insert into TEST1 because it has header columns"));
  }

  @Test
  public void shouldThrowIfSourceDoesNotExist() {
    // Given:
    final Statement stmt = givenQuery("SELECT * FROM UNKNOWN;");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> AstSanitizer.sanitize(stmt, META_STORE)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "UNKNOWN does not exist."));
  }

  @Test
  public void shouldThrowIfLeftJoinSourceDoesNotExist() {
    // Given:
    final Statement stmt = givenQuery("SELECT * FROM UNKNOWN JOIN TEST2"
        + " ON UNKNOWN.col1 = test2.col1;");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> AstSanitizer.sanitize(stmt, META_STORE)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "UNKNOWN does not exist."));
  }

  @Test
  public void shouldThrowIfRightJoinSourceDoesNotExist() {
    // Given:
    final Statement stmt = givenQuery("SELECT * FROM TEST1 JOIN UNKNOWN"
        + " ON test1.col1 = UNKNOWN.col1;");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> AstSanitizer.sanitize(stmt, META_STORE)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "UNKNOWN does not exist."));
  }

  @Test
  public void shouldThrowOnUnknownSource() {
    // Given:
    final Statement stmt = givenQuery("SELECT * FROM Unknown;");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> AstSanitizer.sanitize(stmt, META_STORE)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "UNKNOWN does not exist"));
  }

  @Test
  public void shouldThrowOnUnknownLeftJoinSource() {
    // Given:
    final Statement stmt =
        givenQuery("SELECT * FROM UNKNOWN JOIN TEST2 T2 WITHIN 1 SECOND ON UNKNOWN.ID = T2.ID;");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> AstSanitizer.sanitize(stmt, META_STORE)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "UNKNOWN does not exist"));
  }

  @Test
  public void shouldThrowOnUnknownRightJoinSource() {
    // Given:
    final Statement stmt =
        givenQuery("SELECT * FROM TEST1 T1 JOIN UNKNOWN WITHIN 1 SECOND ON T1.ID = UNKNOWN.ID;");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> AstSanitizer.sanitize(stmt, META_STORE)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "UNKNOWN does not exist"));
  }

  @Test
  public void shouldThrowIfLambdasAreDisabled() {
    // Given:
    final Statement stmt =
        givenQuery("SELECT transform(arr, x => x+1) FROM TEST1;");

    // When:
    final Exception e = assertThrows(
        UnsupportedOperationException.class,
        () -> AstSanitizer.sanitize(stmt, META_STORE, false)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Lambdas are not enabled at this time."));
  }

  @Test
  public void shouldAddQualifierForColumnReference() {
    // Given:
    final Statement stmt = givenQuery("SELECT COL0 FROM TEST1;");

    // When:
    final Query result = (Query) AstSanitizer.sanitize(stmt, META_STORE);

    // Then:
    assertThat(result.getSelect(), is(new Select(ImmutableList.of(
        new SingleColumn(
            column(TEST1_NAME, "COL0"), Optional.of(ColumnName.of("COL0")))
    ))));
  }

  @Test
  public void shouldAddQualifierForJoinColumnReferenceFromLeft() {
    // Given:
    final Statement stmt = givenQuery(
        "SELECT COL5 FROM TEST1 JOIN TEST2 ON TEST1.COL0=TEST2.COL0;");

    // When:
    final Query result = (Query) AstSanitizer.sanitize(stmt, META_STORE);

    // Then:
    assertThat(result.getSelect(), is(new Select(ImmutableList.of(
        new SingleColumn(
            column(TEST1_NAME, "COL5"), Optional.of(ColumnName.of("COL5")))
    ))));
  }

  @Test
  public void shouldAddQualifierForJoinColumnReferenceFromRight() {
    // Given:
    final Statement stmt = givenQuery(
        "SELECT COL5 FROM TEST2 JOIN TEST1 ON TEST2.COL0=TEST1.COL0;");

    // When:
    final Query result = (Query) AstSanitizer.sanitize(stmt, META_STORE);

    // Then:
    assertThat(result.getSelect(), is(new Select(ImmutableList.of(
        new SingleColumn(
            column(TEST1_NAME, "COL5"), Optional.of(ColumnName.of("COL5")))
    ))));
  }

  @Test
  public void shouldAllowDuplicateLambdaArgumentInSeparateExpression() {
    // Given:
    final Statement stmt = givenQuery(
        "SELECT TRANSFORM_ARRAY(Col4, X => X + 5, (X,Y) => Y + 5) FROM TEST1;");

    // When:
    final Query result = (Query) AstSanitizer.sanitize(stmt, META_STORE);

    // Then:
    assertThat(result.getSelect(), is(new Select(ImmutableList.of(
        new SingleColumn(
            new FunctionCall(
                FunctionName.of("TRANSFORM_ARRAY"),
                ImmutableList.of(
                    column(TEST1_NAME, "COL4"),
                    new LambdaFunctionCall(
                        ImmutableList.of("X"),
                        new ArithmeticBinaryExpression(
                            Operator.ADD,
                            new LambdaVariable("X"),
                            new IntegerLiteral(5))
                    ),
                    new LambdaFunctionCall(
                        ImmutableList.of("X", "Y"),
                        new ArithmeticBinaryExpression(
                            Operator.ADD,
                            new LambdaVariable("Y"),
                            new IntegerLiteral(5))
                    )
                )
            ),
            Optional.of(ColumnName.of("KSQL_COL_0")))
    ))));
  }

  @Test
  public void shouldAllowNestedLambdaFunctionsWithoutDuplicate() {
    // Given:
    final Statement stmt = givenQuery(
        "SELECT TRANSFORM_ARRAY(Col4, (X,Y,Z) => TRANSFORM_MAP(Col4, Q => 4, H => 5), (X,Y,Z) => 0) FROM TEST1;");

    // When:
    final Query result = (Query) AstSanitizer.sanitize(stmt, META_STORE);

    // Then:
    assertThat(result.getSelect(), is(new Select(ImmutableList.of(
        new SingleColumn(
            new FunctionCall(
                FunctionName.of("TRANSFORM_ARRAY"),
                ImmutableList.of(
                    column(TEST1_NAME, "COL4"),
                    new LambdaFunctionCall(
                        ImmutableList.of("X", "Y", "Z"),
                        new FunctionCall(
                            FunctionName.of("TRANSFORM_MAP"),
                            ImmutableList.of(
                                column(TEST1_NAME, "COL4"),
                                new LambdaFunctionCall(
                                    ImmutableList.of("Q"),
                                    new IntegerLiteral(4)
                                ),
                                new LambdaFunctionCall(
                                    ImmutableList.of("H"),
                                    new IntegerLiteral(5)
                                )
                            )
                        )
                    ),
                    new LambdaFunctionCall(
                        ImmutableList.of("X", "Y", "Z"),
                        new IntegerLiteral(0)
                    )
                )
            ),
            Optional.of(ColumnName.of("KSQL_COL_0")))
    ))));
  }

  @Test
  public void shouldThrowOnColumnNamesUsedForLambdaArguments() {
    // Given:
    final Statement stmt = givenQuery(
        "SELECT TRANSFORM_ARRAY(Col4, Col0 => Col0 + 5) FROM TEST1;");

    final Exception e = assertThrows(
        KsqlException.class,
        () -> AstSanitizer.sanitize(stmt, META_STORE)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Lambda function argument can't be a column name: COL0"));

  }

  @Test
  public void shouldThrowOnDuplicateLambdaArgumentsInNestedLambda() {
    // Given:
    final Statement stmt1 = givenQuery(
        "SELECT TRANSFORM_ARRAY(Col4, X => TRANSFORM_ARRAY(Col4, X => X)) FROM TEST1;");
    final Statement stmt2 = givenQuery(
        "SELECT TRANSFORM_ARRAY(Col4, (X,Y,Z) => TRANSFORM_MAP(Col4, Q => TRANSFORM_ARRAY(Col4, T => T, X => X))) FROM TEST1;");

    final Exception e1 = assertThrows(
        KsqlException.class,
        () -> AstSanitizer.sanitize(stmt1, META_STORE)
    );
    final Exception e2 = assertThrows(
        KsqlException.class,
        () -> AstSanitizer.sanitize(stmt2, META_STORE)
    );

    // Then:
    assertThat(e1.getMessage(),
        containsString("Reusing lambda arguments in nested lambda is not allowed"));
    assertThat(e2.getMessage(),
        containsString("Reusing lambda arguments in nested lambda is not allowed"));
  }

  @Test
  public void shouldThrowOnAmbiguousQualifierForJoinColumnReference() {
    // Given:
    final Statement stmt = givenQuery(
        "SELECT COL0 FROM TEST1 JOIN TEST2 ON TEST1.COL0=TEST2.COL0;");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> AstSanitizer.sanitize(stmt, META_STORE)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Column 'COL0' is ambiguous. Could be TEST1.COL0 or TEST2.COL0."));
  }

  @Test
  public void shouldPreserveQualifierOnQualifiedColumnReference() {
    // Given:
    final Statement stmt = givenQuery("SELECT TEST1.COL0 FROM TEST1;");

    // When:
    final Query result = (Query) AstSanitizer.sanitize(stmt, META_STORE);

    // Then:
    assertThat(result.getSelect(), is(new Select(ImmutableList.of(
        new SingleColumn(
            column(TEST1_NAME, "COL0"), Optional.of(ColumnName.of("COL0")))
    ))));
  }

  @Test
  public void shouldPreserveQualifierOnAliasQualifiedColumnReference() {
    // Given:
    final Statement stmt = givenQuery("SELECT T.COL0 FROM TEST2 T;");

    // When:
    final Query result = (Query) AstSanitizer.sanitize(stmt, META_STORE);

    // Then:
    assertThat(result.getSelect(), is(new Select(ImmutableList.of(
        new SingleColumn(
            column(SourceName.of("T"), "COL0"), Optional.of(ColumnName.of("COL0")))
    ))));
  }

  @Test
  public void shouldAddAliasForColumnReference() {
    // Given:
    final Statement stmt = givenQuery("SELECT COL0 FROM TEST1;");

    // When:
    final Query result = (Query) AstSanitizer.sanitize(stmt, META_STORE);

    // Then:
    final SingleColumn col = (SingleColumn) result.getSelect().getSelectItems().get(0);
    assertThat(col.getAlias(), equalTo(Optional.of(ColumnName.of("COL0"))));
  }

  @Test
  public void shouldAddAliasForJoinColumnReferenceOfCommonField() {
    // Given:
    final Statement stmt = givenQuery(
        "SELECT TEST1.COL0 FROM TEST1 JOIN TEST2 ON TEST1.COL0=TEST2.COL0;");

    // When:
    final Query result = (Query) AstSanitizer.sanitize(stmt, META_STORE);

    // Then:
    final SingleColumn col = (SingleColumn) result.getSelect().getSelectItems().get(0);
    assertThat(col.getAlias(), equalTo(Optional.of(ColumnName.of("TEST1_COL0"))));
  }

  @Test
  public void shouldAddAliasForStructDereference() {
    // Given:
    final Statement stmt = givenQuery("SELECT ADDRESS->NUMBER FROM ORDERS;");

    // When:
    final Query result = (Query) AstSanitizer.sanitize(stmt, META_STORE);

    // Then:
    final SingleColumn col = (SingleColumn) result.getSelect().getSelectItems().get(0);
    assertThat(col.getAlias(), equalTo(Optional.of(ColumnName.of("NUMBER"))));
  }

  @Test
  public void shouldAddAliasForExpression() {
    // Given:
    final Statement stmt = givenQuery("SELECT 1 + 2 FROM ORDERS;");

    // When:
    final Query result = (Query) AstSanitizer.sanitize(stmt, META_STORE);

    // Then:
    final SingleColumn col = (SingleColumn) result.getSelect().getSelectItems().get(0);
    assertThat(col.getAlias(), equalTo(Optional.of(ColumnName.of("KSQL_COL_0"))));
  }

  @Test
  public void shouldPreserveAliasIfPresent() {
    // Given:
    final Statement stmt = givenQuery("SELECT COL1 AS BOB FROM TEST1;");

    // When:
    final Query result = (Query) AstSanitizer.sanitize(stmt, META_STORE);

    // Then:
    assertThat(result.getSelect(), is(new Select(ImmutableList.of(
        new SingleColumn(column(TEST1_NAME, "COL1"), Optional.of(ColumnName.of("BOB")))
    ))));
  }

  private static Statement givenQuery(final String sql) {
    final List<ParsedStatement> statements = new DefaultKsqlParser().parse(sql);
    assertThat(statements, hasSize(1));
    return new AstBuilder(META_STORE).buildStatement(statements.get(0).getStatement());
  }

  private static QualifiedColumnReferenceExp column(final SourceName source, final String fieldName) {
    return new QualifiedColumnReferenceExp(source, ColumnName.of(fieldName));
  }
}
