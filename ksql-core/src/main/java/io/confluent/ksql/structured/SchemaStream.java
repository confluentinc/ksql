package io.confluent.ksql.structured;

import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.physical.PhysicalPlanBuilder;
import io.confluent.ksql.util.ExpressionUtil;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.codehaus.commons.compiler.IExpressionEvaluator;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.ArrayList;

public class SchemaStream {

    final Schema schema;
    final KStream kStream;
    final Field keyField;

    public SchemaStream(Schema schema, KStream kStream, Field keyField) {
        this.schema = schema;
        this.kStream = kStream;
        this.keyField = keyField;
    }

    public SchemaStream into(String kafkaTopicName) {

        kStream.to(Serdes.String(), PhysicalPlanBuilder.getGenericRowSerde(), kafkaTopicName);
        return  this;
    }

    public SchemaStream filter(Expression filterExpression) throws Exception {
        SQLPredicate predicate = new SQLPredicate(filterExpression, schema);
        KStream filteredKStream = kStream.filter(predicate.getPredicate());
        return new SchemaStream(schema, filteredKStream, keyField);
    }

    public SchemaStream select(Schema selectSchema) {

        KStream projectedKStream = kStream.map(new KeyValueMapper<String, GenericRow, KeyValue<String,GenericRow>>() {
            @Override
            public KeyValue<String, GenericRow> apply(String key, GenericRow row) {
                List<Object> newColumns = new ArrayList();
                for(Field schemaField : selectSchema.fields()) {
                    newColumns.add(row.getColumns().get(SchemaUtil.getFieldIndexByName(schema, schemaField.name())));
                }
                GenericRow newRow = new GenericRow(newColumns);
                return new KeyValue<String, GenericRow>(key, newRow);
            }
        });

        return new SchemaStream(selectSchema, projectedKStream, keyField);
    }

    public SchemaStream select(List<Expression> expressions, Schema selectSchema) throws Exception {
        ExpressionUtil expressionUtil = new ExpressionUtil();
        // TODO: Optimize to remove the code gen for constants and single columns references and use them directly.
        // TODO: Only use code get when we have real expression.
        List<Pair<IExpressionEvaluator, int[]>> expressionEvaluators = new ArrayList<>();
        for(Expression expression: expressions) {
            Pair<IExpressionEvaluator, int[]> expressionEvaluatorPair = expressionUtil.getExpressionEvaluator(expression, schema);
            expressionEvaluators.add(expressionEvaluatorPair);
        }
        KStream projectedKStream = kStream.map(new KeyValueMapper<String, GenericRow, KeyValue<String,GenericRow>>() {
            @Override
            public KeyValue<String, GenericRow> apply(String key, GenericRow row) {
                List<Object> newColumns = new ArrayList();
                for(int i = 0; i < expressions.size(); i++) {
                    Expression expression = expressions.get(i);
                    int[] parameterIndexes = expressionEvaluators.get(i).getRight();
                    Object[] parameterObjects = new Object[parameterIndexes.length];
                    for(int j = 0; j < parameterIndexes.length; j++) {
                        parameterObjects[j] = row.getColumns().get(parameterIndexes[j]);
                    }
                    Object columnValue = null;
                    try {
                        columnValue = expressionEvaluators.get(i).getLeft().evaluate(parameterObjects);
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    newColumns.add(columnValue);

                }
                GenericRow newRow = new GenericRow(newColumns);
                return new KeyValue<String, GenericRow>(key, newRow);
            }
        });

        return new SchemaStream(selectSchema, projectedKStream, keyField);
    }

    public SchemaStream leftJoin(SchemaKTable schemaKTable, Schema joinSchema, Field joinKey) {

        KStream joinedKStream = kStream.leftJoin(schemaKTable.getkTable(), new ValueJoiner<GenericRow, GenericRow, GenericRow>() {
            @Override
            public GenericRow apply(GenericRow leftGenericRow, GenericRow rightGenericRow) {
                List<Object> columns = new ArrayList<>();
                columns.addAll(leftGenericRow.getColumns());
                if(rightGenericRow == null) {
                    for (int i = leftGenericRow.getColumns().size(); i < joinSchema.fields().size(); i++) {
                        columns.add(null);
                    }
                } else {
                    columns.addAll(rightGenericRow.getColumns());
                }

                GenericRow joinGenericRow = new GenericRow(columns);
                return joinGenericRow;
            }
        });

        return new SchemaStream(joinSchema, joinedKStream, joinKey);
    }

    public SchemaStream selectKey(Field newKeyField) {
        if (keyField.name().equalsIgnoreCase(newKeyField.name())) {
            return this;
        }

        KStream keyedKStream = kStream.selectKey(new KeyValueMapper<String, GenericRow, String>() {
            @Override
            public String apply(String key, GenericRow value) {

                String newKey = value.getColumns().get(SchemaUtil.getFieldIndexByName(schema, newKeyField.name())).toString();
//                return new KeyValue<String, GenericRow>(newKey, value);
                return newKey;
            }
        });

        return new SchemaStream(schema, keyedKStream, newKeyField);
    }

    public Field getKeyField() {
        return keyField;
    }

    public Schema getSchema() {
        return schema;
    }

    public KStream getkStream() {
        return kStream;
    }
}
