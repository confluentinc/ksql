/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.embedded;

import io.confluent.ksql.KsqlContext;

public class EmbeddedKsql {

  public static void main(String[] args) throws Exception {

    KsqlContext ksqlContext = new KsqlContext();

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
