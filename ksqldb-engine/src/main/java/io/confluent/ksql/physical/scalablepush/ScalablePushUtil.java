package io.confluent.ksql.physical.scalablepush;

import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class ScalablePushUtil {

  public static boolean isScalablePushQuery(final Statement statement,
      final KsqlConfig ksqlConfig, final Map<String, Object> overrides) {
    if (! (statement instanceof Query)) {
      return false;
    }
    final Query query = (Query) statement;
    final boolean isLatest = overrides.containsKey("auto.offset.reset")
        ? "latest".equals(overrides.get("auto.offset.reset"))
        : "latest".equals(ksqlConfig.getKsqlStreamConfigProp("auto.offset.reset"));
    return !query.isPullQuery()
        && !query.getGroupBy().isPresent()
        && !query.getWindow().isPresent()
        && !query.getHaving().isPresent()
        && !query.getPartitionBy().isPresent()
        && isLatest;
  }
}
