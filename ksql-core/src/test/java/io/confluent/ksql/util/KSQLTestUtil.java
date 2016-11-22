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
                .field("col0".toUpperCase(), SchemaBuilder.INT64_SCHEMA)
                .field("col1".toUpperCase(), SchemaBuilder.STRING_SCHEMA)
                .field("col2".toUpperCase(), SchemaBuilder.STRING_SCHEMA)
                .field("col3".toUpperCase(), SchemaBuilder.FLOAT64_SCHEMA);

        KafkaTopic kafkaTopic1 = new KafkaTopic("test1", schemaBuilder1, schemaBuilder1.field("col0"), DataSource.DataSourceType.KSTREAM, "test1-topic");
        metaStore.putSource(kafkaTopic1);

        SchemaBuilder schemaBuilder2 = SchemaBuilder.struct()
                .field("col0".toUpperCase(), SchemaBuilder.INT64_SCHEMA)
                .field("col1".toUpperCase(), SchemaBuilder.STRING_SCHEMA)
                .field("col2".toUpperCase(), SchemaBuilder.STRING_SCHEMA)
                .field("col3".toUpperCase(), SchemaBuilder.FLOAT64_SCHEMA)
                .field("col4".toUpperCase(), SchemaBuilder.BOOLEAN_SCHEMA);

        KafkaTopic kafkaTopic2 = new KafkaTopic("test2", schemaBuilder2, schemaBuilder2.field("col0"), DataSource.DataSourceType.KSTREAM, "test2-topic");
        metaStore.putSource(kafkaTopic2);
        return  metaStore;
    }
}
