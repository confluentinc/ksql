package io.confluent.kql.structured;

import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.util.ExpressionUtil;
import io.confluent.kql.util.GenericRowValueTypeEnforcer;
import io.confluent.kql.util.SchemaUtil;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Predicate;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IExpressionEvaluator;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class SQLPredicate {

  Expression filterExpression;
  final Schema schema;
  IExpressionEvaluator ee;
  int[] columnIndexes;

  GenericRowValueTypeEnforcer genericRowValueTypeEnforcer;

  public SQLPredicate(Expression filterExpression, Schema schema) throws Exception {
    this.filterExpression = filterExpression;
    this.schema = schema;
    this.genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schema);

    ExpressionUtil expressionUtil = new ExpressionUtil();
    Map<String, Class> parameterMap = expressionUtil.getParameterInfo(filterExpression, schema);

    String[] parameterNames = new String[parameterMap.size()];
    Class[] parameterTypes = new Class[parameterMap.size()];
    columnIndexes = new int[parameterMap.size()];

    int index = 0;
    for (String parameterName : parameterMap.keySet()) {
      parameterNames[index] = parameterName;
      parameterTypes[index] = parameterMap.get(parameterName);
      columnIndexes[index] = SchemaUtil.getFieldIndexByName(schema, parameterName);
      index++;
    }

    String expressionStr = filterExpression.getCodegenString(schema);
    ee = CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();

    // The expression will have two "int" parameters: "a" and "b".
    ee.setParameters(parameterNames, parameterTypes);

    // And the expression (i.e. "result") type is also "int".
    ee.setExpressionType(boolean.class);

    // And now we "cook" (scan, parse, compile and load) the fabulous expression.
    ee.cook(expressionStr);
  }

  public Predicate getPredicate() {
    return new Predicate<String, GenericRow>() {
      @Override
      public boolean test(String key, GenericRow row) {
        try {
          Object[] values = new Object[columnIndexes.length];
          for (int i = 0; i < values.length; i++) {
            values[i] = genericRowValueTypeEnforcer.enforceFieldType(columnIndexes[i],row
                .getColumns().get(columnIndexes[i]));
          }
          boolean result = (Boolean) ee.evaluate(values);
          return result;
        } catch (InvocationTargetException e) {
          e.printStackTrace();
        }
        throw new RuntimeException("Invalid format.");
      }
    };
  }

}
