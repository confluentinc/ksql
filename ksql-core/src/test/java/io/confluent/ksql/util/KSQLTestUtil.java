package io.confluent.ksql.util;


import io.confluent.ksql.metastore.KQLStream;
import io.confluent.ksql.metastore.KQLTable;
import io.confluent.ksql.metastore.KQLTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;

import org.apache.kafka.connect.data.SchemaBuilder;

public class KSQLTestUtil {

  public static MetaStore getNewMetaStore() {

    MetaStore metaStore = new MetaStoreImpl();

    SchemaBuilder schemaBuilder1 = SchemaBuilder.struct()
        .field("COL0", SchemaBuilder.INT64_SCHEMA)
        .field("COL1", SchemaBuilder.STRING_SCHEMA)
        .field("COL2", SchemaBuilder.STRING_SCHEMA)
        .field("COL3", SchemaBuilder.FLOAT64_SCHEMA);

    KQLTopic
        KQLTopic1 =
        new KQLTopic("test1", "test1", null);

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
        new KQLTopic("test2", "test2", null);
    KQLTable kqlTable = new KQLTable("test2", schemaBuilder2, schemaBuilder2.field("COL0"),
                                     KQLTopic2, "test2");

    metaStore.putTopic(KQLTopic2);
    metaStore.putSource(kqlTable);
    return metaStore;
  }
}
