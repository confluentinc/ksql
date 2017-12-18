/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.embedded;

import java.util.Collections;

import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.util.KsqlConfig;

public class EmbeddedKsql {

  public static void main(String[] args) throws Exception {

    KsqlContext ksqlContext = KsqlContext.create(new KsqlConfig(Collections.emptyMap()));

    ksqlContext.sql("REGISTER TOPIC orders_topic WITH (format = 'json', "
                    + "kafka_topic='orders_topic_json');");

    ksqlContext.sql("CREATE STREAM orders (ordertime bigint, orderid bigint, itemid varchar, "
                    + "orderunits double, arraycol array<double>, mapcol map<varchar, double>) "
                    + "WITH (topicname = 'orders_topic' , key='orderid');\n");
    ksqlContext.sql("CREATE STREAM BIGORDERS AS SELECT * FROM ORDERS WHERE ORDERUNITS > 5;");
    ksqlContext.sql("SELECT * FROM ORDERS;");
    ksqlContext.sql("CREATE TABLE ORDERSUMS AS select itemid, sum(orderunits) from orders window "
                    + "TUMBLING ( size 30 second) group by itemid;");

    System.out.println("Queries are running!");

  }

}
