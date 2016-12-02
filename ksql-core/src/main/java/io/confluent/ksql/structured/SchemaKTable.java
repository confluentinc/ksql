package io.confluent.ksql.structured;

import io.confluent.ksql.function.udf.KUDF;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.physical.PhysicalPlanBuilder;
import io.confluent.ksql.util.ExpressionUtil;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.Triplet;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.codehaus.commons.compiler.IExpressionEvaluator;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class SchemaKTable extends SchemaKStream {

  final KTable kTable;

  public SchemaKTable(Schema schema, KTable kTable, Field keyField) {
    super(schema, null, keyField);
    this.kTable = kTable;
  }

  @Override
  public SchemaKTable into(String kafkaTopicName) {

    kTable.to(Serdes.String(), PhysicalPlanBuilder.getGenericRowSerde(), kafkaTopicName);
    return this;
  }

  @Override
  public SchemaKTable filter(Expression filterExpression) throws Exception {
    SQLPredicate predicate = new SQLPredicate(filterExpression, schema);
    KTable filteredKTable = kTable.filter(predicate.getPredicate());
    return new SchemaKTable(schema, filteredKTable, keyField);
  }

  @Override
  public SchemaKTable select(List<Expression> expressions, Schema selectSchema) throws Exception {
    ExpressionUtil expressionUtil = new ExpressionUtil();
    // TODO: Optimize to remove the code gen for constants and single columns references and use them directly.
    // TODO: Only use code get when we have real expression.
    List<Triplet<IExpressionEvaluator, int[], KUDF[]>> expressionEvaluators = new ArrayList<>();
    for (Expression expression : expressions) {
      Triplet<IExpressionEvaluator, int[], KUDF[]>
          expressionEvaluatorPair =
          expressionUtil.getExpressionEvaluator(expression, schema);
      expressionEvaluators.add(expressionEvaluatorPair);
    }

    KTable projectedKTable = kTable.mapValues(new ValueMapper<GenericRow, GenericRow>() {
      @Override
      public GenericRow apply(GenericRow row) {
        List<Object> newColumns = new ArrayList();
        for (int i = 0; i < expressions.size(); i++) {
          int[] parameterIndexes = expressionEvaluators.get(i).getSecond();
          Object[] parameterObjects = new Object[parameterIndexes.length];
          for (int j = 0; j < parameterIndexes.length; j++) {
            parameterObjects[j] = row.getColumns().get(parameterIndexes[j]);
          }
          Object columnValue = null;
          try {
            columnValue = expressionEvaluators.get(i).getFirst().evaluate(parameterObjects);
          } catch (InvocationTargetException e) {
            e.printStackTrace();
          }
          newColumns.add(columnValue);
        }
        GenericRow newRow = new GenericRow(newColumns);
        return newRow;
      }
    });

    return new SchemaKTable(selectSchema, projectedKTable, keyField);
  }

  public KTable getkTable() {
    return kTable;
  }

}
