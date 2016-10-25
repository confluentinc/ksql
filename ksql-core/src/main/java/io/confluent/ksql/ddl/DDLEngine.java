package io.confluent.ksql.ddl;


import io.confluent.ksql.KSQLEngine;
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KafkaTopic;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.util.KSQLException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.ArrayList;
import java.util.List;

public class DDLEngine {

    KSQLEngine ksqlEngine;

    public DDLEngine(KSQLEngine ksqlEngine) {
        this.ksqlEngine = ksqlEngine;
    }

    public void createTopic(CreateTable createTable) {

        String topicName = createTable.getName().getSuffix();

        SchemaBuilder topicSchema = SchemaBuilder.struct();
        List<Field> topicSchemaFields = new ArrayList<>();
        for (TableElement tableElement: createTable.getElements()) {
            topicSchema = topicSchema.field(tableElement.getName(), getKSQLType(tableElement.getType()));
        }

        // Create the topic in Kafka

        // Add the topic to the metastore
        KafkaTopic kafkaTopic = new KafkaTopic(topicName, topicSchema, DataSource.DataSourceType.STREAM, topicName);
        ksqlEngine.getMetaStore().putSource(kafkaTopic);
        new DDLUtil().createTopic(topicName, 3, 1);

    }

    public void dropTopic(DropTable dropTable) {

        String topicName = dropTable.getTableName().getSuffix();
        ksqlEngine.getMetaStore().deleteSource(topicName);
        new DDLUtil().deleteTopic(topicName);
    }

    //TODO: this needs to be moved to proper place to be accessible to everyone. Temporary!
    private Schema getKSQLType(String sqlType) {
        if (sqlType.equalsIgnoreCase("BIGINT")) {
            return Schema.INT64_SCHEMA;
        } else if (sqlType.equalsIgnoreCase("VARCHAR")) {
            return Schema.STRING_SCHEMA;
        } else if (sqlType.equalsIgnoreCase("DOUBLE")) {
            return Schema.FLOAT64_SCHEMA;
        } else if (sqlType.equalsIgnoreCase("INTEGER") || sqlType.equalsIgnoreCase("INT")) {
            return Schema.INT32_SCHEMA;
        } else if (sqlType.equalsIgnoreCase("BOOELAN") || sqlType.equalsIgnoreCase("BOOL")) {
            return Schema.BOOLEAN_SCHEMA;
        }
        throw new KSQLException("Unsupported type: "+sqlType);
    }

}
