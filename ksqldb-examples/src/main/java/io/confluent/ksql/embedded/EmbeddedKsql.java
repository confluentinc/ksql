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

package io.confluent.ksql.embedded;

import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;

public final class EmbeddedKsql {

  private EmbeddedKsql() {
  }

  public static void main(final String[] args) {

    final KsqlContext ksqlContext = KsqlContext.create(
        new KsqlConfig(Collections.emptyMap()),
        ProcessingLogContext.create(),
        new MetricCollectors());

    ksqlContext.sql("REGISTER TOPIC orders_topic WITH (format = 'json', "
                    + "kafka_topic='orders_topic_json');");

    ksqlContext.sql("CREATE STREAM orders (orderid bigint KEY, ordertime bigint, itemid varchar, "
                    + "orderunits double, arraycol array<double>, mapcol map<varchar, double>) "
                    + "WITH (topicname = 'orders_topic' );\n");
    ksqlContext.sql("CREATE STREAM BIGORDERS AS SELECT * FROM ORDERS WHERE ORDERUNITS > 5;");
    ksqlContext.sql("SELECT * FROM ORDERS;");
    ksqlContext.sql("CREATE TABLE ORDERSUMS AS select itemid, sum(orderunits) from orders window "
                    + "TUMBLING ( size 30 second) group by itemid;");

    System.out.println("Queries are running!");
  }
}
