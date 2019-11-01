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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.AstBuilder;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.List;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class AstSanitizerTest {

  private static final MetaStore META_STORE = MetaStoreFixture
      .getNewMetaStore(mock(FunctionRegistry.class));

  private static final SourceName TEST1_NAME = SourceName.of("TEST1");
  private static final SourceName TEST2_NAME = SourceName.of("TEST2");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldThrowIfSourceDoesNotExist() {
    // Given:
    final Statement stmt = givenQuery("SELECT * FROM UNKNOWN;");

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("UNKNOWN does not exist.");

    // When:
    AstSanitizer.sanitize(stmt, META_STORE);
  }

  @Test
  public void shouldThrowIfLeftJoinSourceDoesNotExist() {
    // Given:
    final Statement stmt = givenQuery("SELECT * FROM UNKNOWN JOIN TEST2"
        + " ON UNKNOWN.col1 = test2.col1;");
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("UNKNOWN does not exist.");

    // When:
    AstSanitizer.sanitize(stmt, META_STORE);
  }

  @Test
  public void shouldThrowIfRightJoinSourceDoesNotExist() {
    // Given:
    final Statement stmt = givenQuery("SELECT * FROM TEST1 JOIN UNKNOWN"
        + " ON test1.col1 = UNKNOWN.col1;");

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("UNKNOWN does not exist.");

    // When:
    AstSanitizer.sanitize(stmt, META_STORE);
  }

  @Test
  public void shouldThrowOnUnknownSource() {
    // Given:
    final Statement stmt = givenQuery("SELECT * FROM Unknown;");

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("UNKNOWN does not exist");

    // When:
    AstSanitizer.sanitize(stmt, META_STORE);
  }

  @Test
  public void shouldThrowOnUnknownLeftJoinSource() {
    // Given:
    final Statement stmt =
        givenQuery("SELECT * FROM UNKNOWN JOIN TEST2 T2 WITHIN 1 SECOND ON UNKNOWN.ID = T2.ID;");

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("UNKNOWN does not exist");

    // When:
    AstSanitizer.sanitize(stmt, META_STORE);
  }

  @Test
  public void shouldThrowOnUnknownRightJoinSource() {
    // Given:
    final Statement stmt =
        givenQuery("SELECT * FROM TEST1 T1 JOIN UNKNOWN WITHIN 1 SECOND ON T1.ID = UNKNOWN.ID;");

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("UNKNOWN does not exist");

    // When:
    AstSanitizer.sanitize(stmt, META_STORE);
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
  public void shouldThrowOnAmbiguousQualifierForJoinColumnReference() {
    // Given:
    final Statement stmt = givenQuery(
        "SELECT COL0 FROM TEST1 JOIN TEST2 ON TEST1.COL0=TEST2.COL0;");

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Column 'COL0' is ambiguous.");

    // When:
    AstSanitizer.sanitize(stmt, META_STORE);
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
    assertThat(col.getAlias(), equalTo(Optional.of(ColumnName.of("ADDRESS__NUMBER"))));
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
    return new AstBuilder(META_STORE).build(statements.get(0).getStatement());
  }

  private static ColumnReferenceExp column(final SourceName source, final String fieldName) {
    return new ColumnReferenceExp(ColumnRef.of(source, ColumnName.of(fieldName)));
  }
}