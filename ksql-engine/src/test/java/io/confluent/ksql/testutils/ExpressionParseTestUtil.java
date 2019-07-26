package io.confluent.ksql.testutils;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SingleColumn;

public final class ExpressionParseTestUtil {
  public static Expression parseExpression(final String asText, final MetaStore metaStore) {
    final KsqlParser parser = new DefaultKsqlParser();
    final String ksql = String.format("SELECT %s FROM test1;", asText);

    final ParsedStatement parsedStatement = parser.parse(ksql).get(0);
    final PreparedStatement preparedStatement = parser.prepare(parsedStatement, metaStore);
    final SingleColumn singleColumn = (SingleColumn) ((Query)preparedStatement.getStatement())
        .getSelect()
        .getSelectItems()
        .get(0);
    return singleColumn.getExpression();
  }
}
