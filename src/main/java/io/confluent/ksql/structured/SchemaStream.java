package io.confluent.ksql.structured;

import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.physical.PhysicalPlanBuilder;
import io.confluent.ksql.planner.Schema;
import io.confluent.ksql.planner.SchemaField;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class SchemaStream {
    final Schema schema;
    final KStream kStream;

    public SchemaStream(Schema schema, KStream kStream) {
        this.schema = schema;
        this.kStream = kStream;
    }

    public SchemaStream into(String kafkaTopicName) {

        kStream.to(Serdes.String(), PhysicalPlanBuilder.getGenericRowSerde(), kafkaTopicName);
        return  this;
    }

    public SchemaStream filter(ComparisonExpression comparisonExpression) {
        SQLPredicate predicate = new SQLPredicate(comparisonExpression, schema);
        KStream filteredKStream = kStream.filter(predicate.getPredicate());
        return new SchemaStream(schema.duplicate(), filteredKStream);
    }

    public SchemaStream select(Schema selectSchema) {
        KStream projectedKStream = kStream.map(new KeyValueMapper<String, GenericRow, KeyValue<String,GenericRow>>() {
            @Override
            public KeyValue<String, GenericRow> apply(String key, GenericRow row) {
                List<Object> newColumns = new ArrayList();
                for(SchemaField schemaField : selectSchema.getSchemaFields()) {
                    newColumns.add(row.getColumns().get(schema.getFieldIndexByName(schemaField.getFieldName())));
                }
                GenericRow newRow = new GenericRow(newColumns);
                return new KeyValue<String, GenericRow>(key, newRow);
            }
        });

        return new SchemaStream(selectSchema, projectedKStream);
    }

    public Schema getSchema() {
        return schema;
    }

    public KStream getkStream() {
        return kStream;
    }
}
