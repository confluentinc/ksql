package io.confluent.ksql.util;


import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KafkaTopic;
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

    KafkaTopic
        kafkaTopic1 =
        new KafkaTopic("test1", schemaBuilder1, schemaBuilder1.field("COL0"),
                       DataSource.DataSourceType.KSTREAM, "test1-topic");
    metaStore.putSource(kafkaTopic1);

    SchemaBuilder schemaBuilder2 = SchemaBuilder.struct()
        .field("COL0", SchemaBuilder.INT64_SCHEMA)
        .field("COL1", SchemaBuilder.STRING_SCHEMA)
        .field("COL2", SchemaBuilder.STRING_SCHEMA)
        .field("COL3", SchemaBuilder.FLOAT64_SCHEMA)
        .field("COL4", SchemaBuilder.BOOLEAN_SCHEMA);

    KafkaTopic
        kafkaTopic2 =
        new KafkaTopic("test2", schemaBuilder2, schemaBuilder2.field("COL0"),
                       DataSource.DataSourceType.KSTREAM, "test2-topic");
    metaStore.putSource(kafkaTopic2);
    return metaStore;
  }
}
