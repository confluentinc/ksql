package io.confluent.ksql.physical.scalable_push;

import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import java.util.function.Supplier;

public class ScalablePushUtil {

  public static boolean isScalablePushQuery(Supplier<? extends Statement> statementSupplier) {
    if (! (statementSupplier.get() instanceof Query)) {
      return false;
    }
    final Query query = (Query) statementSupplier.get();
    return !query.isPullQuery()
        && !query.getGroupBy().isPresent()
        && !query.getWindow().isPresent()
        && !query.getHaving().isPresent()
        && !query.getPartitionBy().isPresent();
  }
}
