/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.physical.scalablepush;

import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;

public final class ScalablePushUtil {

  private ScalablePushUtil() { }

  @SuppressWarnings({"BooleanExpressionComplexity"})
  public static boolean isScalablePushQuery(final Statement statement,
      final KsqlConfig ksqlConfig, final Map<String, Object> overrides) {
    if (!ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PUSH_SCALABLE_ENABLED)) {
      return false;
    }
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
