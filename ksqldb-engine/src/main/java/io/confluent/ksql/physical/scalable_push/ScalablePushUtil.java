package io.confluent.ksql.physical.scalable_push;

import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;

public class ScalablePushUtil {

  public static boolean isScalablePushQuery(final PreparedStatement<?> statement) {
    if (! (statement.getStatement() instanceof Query)) {
      return false;
    }
    final Query query = (Query) statement.getStatement();
    return !query.isPullQuery()
        && !query.getGroupBy().isPresent()
        && !query.getWindow().isPresent()
        && !query.getHaving().isPresent()
        && !query.getPartitionBy().isPresent();
  }
}
