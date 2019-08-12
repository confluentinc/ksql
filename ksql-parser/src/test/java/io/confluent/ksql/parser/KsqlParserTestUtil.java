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

package io.confluent.ksql.parser;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import java.util.List;
import java.util.stream.Collectors;

public final class KsqlParserTestUtil {

  private static final KsqlParser KSQL_PARSER = new DefaultKsqlParser();

  private KsqlParserTestUtil() {
  }

  @SuppressWarnings("unchecked")
  public static <T extends Statement> PreparedStatement<T> buildSingleAst(
      final String sql,
      final MetaStore metaStore
  ) {
    final List<PreparedStatement<?>> statements = buildAst(sql, metaStore);
    assertThat(statements, hasSize(1));
    return (PreparedStatement<T>)statements.get(0);
  }

  public static List<PreparedStatement<?>> buildAst(final String sql, final MetaStore metaStore) {
    return parse(sql).stream()
        .map(parsed -> KSQL_PARSER.prepare(parsed, metaStore))
        .collect(Collectors.toList());
  }

  public static List<ParsedStatement> parse(final String sql) {
    return KSQL_PARSER.parse(sql);
  }

  public static Expression parseExpression(final String asText, final MetaStore metaStore) {
    final String ksql = String.format("SELECT %s FROM test1;", asText);

    final ParsedStatement parsedStatement = KSQL_PARSER.parse(ksql).get(0);
    final AstBuilder astBuilder = new AstBuilder(metaStore);
    final Statement statement = astBuilder.build(parsedStatement.getStatement());
    final SingleColumn singleColumn = (SingleColumn) ((Query)statement)
        .getSelect()
        .getSelectItems()
        .get(0);
    return singleColumn.getExpression();
  }
}
