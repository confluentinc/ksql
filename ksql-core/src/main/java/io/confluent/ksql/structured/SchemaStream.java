package io.confluent.ksql.structured;

import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.physical.PhysicalPlanBuilder;
import io.confluent.ksql.planner.Schema;
import io.confluent.ksql.planner.SchemaField;
import io.confluent.ksql.util.ExpressionUtil;
import io.confluent.ksql.util.Pair;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.codehaus.commons.compiler.IExpressionEvaluator;

import java.lang.reflect.InvocationTargetException;
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

    public SchemaStream filter(Expression filterExpression) throws Exception {
        SQLPredicate predicate = new SQLPredicate(filterExpression, schema);
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

        return new SchemaStream(selectSchema, projectedKStream);
    }


    public Schema getSchema() {
        return schema;
    }

    public KStream getkStream() {
        return kStream;
    }
}
