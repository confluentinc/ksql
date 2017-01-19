package io.confluent.kql.util;


import io.confluent.kql.metastore.KQLStream;
import io.confluent.kql.metastore.KQLTable;
import io.confluent.kql.metastore.KQLTopic;
import io.confluent.kql.metastore.MetaStore;
import io.confluent.kql.metastore.MetaStoreImpl;

import io.confluent.kql.serde.json.KQLJsonTopicSerDe;
import org.apache.kafka.connect.data.SchemaBuilder;

public class KQLTestUtil {

  public static MetaStore getNewMetaStore() {

    MetaStore metaStore = new MetaStoreImpl();

    SchemaBuilder schemaBuilder1 = SchemaBuilder.struct()
        .field("COL0", SchemaBuilder.INT64_SCHEMA)
        .field("COL1", SchemaBuilder.STRING_SCHEMA)
        .field("COL2", SchemaBuilder.STRING_SCHEMA)
        .field("COL3", SchemaBuilder.FLOAT64_SCHEMA);

    KQLTopic
        KQLTopic1 =
        new KQLTopic("test1", "test1", new KQLJsonTopicSerDe());

    KQLStream kqlStream = new KQLStream("test1", schemaBuilder1, schemaBuilder1.field("COL0"),
                                        KQLTopic1);

    metaStore.putTopic(KQLTopic1);
    metaStore.putSource(kqlStream);

    SchemaBuilder schemaBuilder2 = SchemaBuilder.struct()
        .field("COL0", SchemaBuilder.INT64_SCHEMA)
        .field("COL1", SchemaBuilder.STRING_SCHEMA)
        .field("COL2", SchemaBuilder.STRING_SCHEMA)
        .field("COL3", SchemaBuilder.FLOAT64_SCHEMA)
        .field("COL4", SchemaBuilder.BOOLEAN_SCHEMA);

    KQLTopic
        KQLTopic2 =
        new KQLTopic("test2", "test2", new KQLJsonTopicSerDe());
    KQLTable kqlTable = new KQLTable("test2", schemaBuilder2, schemaBuilder2.field("COL0"),
                                     KQLTopic2, "test2");

    metaStore.putTopic(KQLTopic2);
    metaStore.putSource(kqlTable);

    SchemaBuilder schemaBuilderOrders = SchemaBuilder.struct()
            .field("ordertime", SchemaBuilder.INT64_SCHEMA)
            .field("orderid", SchemaBuilder.STRING_SCHEMA)
            .field("itemid", SchemaBuilder.STRING_SCHEMA)
            .field("orderunits", SchemaBuilder.FLOAT64_SCHEMA);

    KQLTopic
            KQLTopicOrders =
            new KQLTopic("orders_topic", "orders_topic", new KQLJsonTopicSerDe());

    KQLStream kqlStreamOrders = new KQLStream("orders", schemaBuilderOrders, schemaBuilderOrders.field("ordertime"),
            KQLTopic1);

    metaStore.putTopic(KQLTopicOrders);
    metaStore.putSource(kqlStreamOrders);

    return metaStore;
  }
}
