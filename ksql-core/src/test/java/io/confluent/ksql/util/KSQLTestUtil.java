package io.confluent.ksql.util;


import io.confluent.ksql.metastore.KQLStream;
import io.confluent.ksql.metastore.KQLTable;
import io.confluent.ksql.metastore.KafkaTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.StructuredDataSource;

import org.apache.kafka.connect.data.SchemaBuilder;

public class KSQLTestUtil {

  public static MetaStore getNewMetaStore() {

    MetaStore metaStore = new MetaStoreImpl();

    SchemaBuilder schemaBuilder1 = SchemaBuilder.struct()
        .field("COL0", SchemaBuilder.INT64_SCHEMA)
        .field("COL1", SchemaBuilder.STRING_SCHEMA)
        .field("COL2", SchemaBuilder.STRING_SCHEMA)
        .field("COL3", SchemaBuilder.FLOAT64_SCHEMA);

    KafkaTopic
        kafkaTopic1 =
        new KafkaTopic("test1", "test1", null);

    KQLStream kqlStream = new KQLStream("test1", schemaBuilder1, schemaBuilder1.field("COL0"),
                                        kafkaTopic1);

    metaStore.putTopic(kafkaTopic1);
    metaStore.putSource(kqlStream);

    SchemaBuilder schemaBuilder2 = SchemaBuilder.struct()
        .field("COL0", SchemaBuilder.INT64_SCHEMA)
        .field("COL1", SchemaBuilder.STRING_SCHEMA)
        .field("COL2", SchemaBuilder.STRING_SCHEMA)
        .field("COL3", SchemaBuilder.FLOAT64_SCHEMA)
        .field("COL4", SchemaBuilder.BOOLEAN_SCHEMA);

    KafkaTopic
        kafkaTopic2 =
        new KafkaTopic("test2", "test2", null);
    KQLTable kqlTable = new KQLTable("test2", schemaBuilder2, schemaBuilder2.field("COL0"),
                                     kafkaTopic2, "test2");

    metaStore.putTopic(kafkaTopic2);
    metaStore.putSource(kqlTable);
    return metaStore;
  }
}
