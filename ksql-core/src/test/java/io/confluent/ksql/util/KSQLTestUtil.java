package io.confluent.ksql.util;


import io.confluent.ksql.metastore.KSQLStream;
import io.confluent.ksql.metastore.KSQLTable;
import io.confluent.ksql.metastore.KSQLTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.serde.json.KSQLJsonTopicSerDe;
import org.apache.kafka.connect.data.SchemaBuilder;

public class KSQLTestUtil {

  public static MetaStore getNewMetaStore() {

    MetaStore metaStore = new MetaStoreImpl();

    SchemaBuilder schemaBuilder1 = SchemaBuilder.struct()
        .field("COL0", SchemaBuilder.INT64_SCHEMA)
        .field("COL1", SchemaBuilder.STRING_SCHEMA)
        .field("COL2", SchemaBuilder.STRING_SCHEMA)
        .field("COL3", SchemaBuilder.FLOAT64_SCHEMA)
        .field("COL4", SchemaBuilder.array(SchemaBuilder.FLOAT64_SCHEMA))
        .field("COL5", SchemaBuilder.map(SchemaBuilder.STRING_SCHEMA, SchemaBuilder.FLOAT64_SCHEMA));

    KSQLTopic
        ksqlTopic1 =
        new KSQLTopic("TEST1", "test1", new KSQLJsonTopicSerDe());

    KSQLStream ksqlStream = new KSQLStream("TEST1", schemaBuilder1, schemaBuilder1.field("COL0"),
        ksqlTopic1);

    metaStore.putTopic(ksqlTopic1);
    metaStore.putSource(ksqlStream);

    SchemaBuilder schemaBuilder2 = SchemaBuilder.struct()
        .field("COL0", SchemaBuilder.INT64_SCHEMA)
        .field("COL1", SchemaBuilder.STRING_SCHEMA)
        .field("COL2", SchemaBuilder.STRING_SCHEMA)
        .field("COL3", SchemaBuilder.FLOAT64_SCHEMA)
        .field("COL4", SchemaBuilder.BOOLEAN_SCHEMA);

    KSQLTopic
        ksqlTopic2 =
        new KSQLTopic("TEST2", "test2", new KSQLJsonTopicSerDe());
    KSQLTable ksqlTable = new KSQLTable("TEST2", schemaBuilder2, schemaBuilder2.field("COL0"),
        ksqlTopic2, "TEST2", false);

    metaStore.putTopic(ksqlTopic2);
    metaStore.putSource(ksqlTable);

    SchemaBuilder schemaBuilderOrders = SchemaBuilder.struct()
            .field("ORDERTIME", SchemaBuilder.INT64_SCHEMA)
            .field("ORDERID", SchemaBuilder.STRING_SCHEMA)
            .field("ITEMID", SchemaBuilder.STRING_SCHEMA)
            .field("ORDERUNITS", SchemaBuilder.FLOAT64_SCHEMA);

    KSQLTopic
        ksqlTopicOrders =
            new KSQLTopic("ORDERS_TOPIC", "orders_topic", new KSQLJsonTopicSerDe());

    KSQLStream ksqlStreamOrders = new KSQLStream("ORDERS", schemaBuilderOrders, schemaBuilderOrders.field("ORDERTIME"),
        ksqlTopicOrders);

    metaStore.putTopic(ksqlTopicOrders);
    metaStore.putSource(ksqlStreamOrders);

    return metaStore;
  }
}
