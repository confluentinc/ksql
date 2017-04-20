/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.structured;

import io.confluent.kql.function.udf.KUDF;
import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.util.ExpressionMetadata;
import io.confluent.kql.util.ExpressionUtil;
import io.confluent.kql.util.GenericRowValueTypeEnforcer;
import io.confluent.kql.util.SchemaUtil;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Windowed;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IExpressionEvaluator;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class SQLPredicate {

  Expression filterExpression;
  final Schema schema;
  IExpressionEvaluator ee;
  int[] columnIndexes;
  boolean isWindowedKey;

  GenericRowValueTypeEnforcer genericRowValueTypeEnforcer;

  public SQLPredicate(final Expression filterExpression, final Schema schema, boolean isWindowedKey) throws Exception {
    this.filterExpression = filterExpression;
    this.schema = schema;
    this.genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schema);
    this.isWindowedKey = isWindowedKey;

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
    if (isWindowedKey) {
      return getWindowedKeyPredicate();
    } else {
      return getStringKeyPredicate();
    }
  }

  private Predicate getStringKeyPredicate() {
    ExpressionUtil expressionUtil = new ExpressionUtil();
    return new Predicate<String, GenericRow>() {
      @Override
      public boolean test(String key, GenericRow row) {
        try {
          ExpressionMetadata
              expressionEvaluator =
              expressionUtil.getExpressionEvaluator(filterExpression, schema);
          KUDF[] kudfs = expressionEvaluator.getUdfs();
          Object[] values = new Object[columnIndexes.length];
          for (int i = 0; i < values.length; i++) {
            if (columnIndexes[i] < 0) {
              values[i] = kudfs[i];
            } else {
              values[i] = genericRowValueTypeEnforcer.enforceFieldType(columnIndexes[i], row
                  .getColumns().get(columnIndexes[i]));
            }
          }
          boolean result = (Boolean) ee.evaluate(values);
          return result;
        } catch (InvocationTargetException e) {
          e.printStackTrace();
        } catch (Exception e) {
          e.printStackTrace();
        }
        throw new RuntimeException("Invalid format.");
      }
    };
  }

  private Predicate getWindowedKeyPredicate() {
    ExpressionUtil expressionUtil = new ExpressionUtil();
    return new Predicate<Windowed<String>, GenericRow>() {
      @Override
      public boolean test(Windowed<String> key, GenericRow row) {
        try {
          ExpressionMetadata
              expressionEvaluator =
              expressionUtil.getExpressionEvaluator(filterExpression, schema);
          KUDF[] kudfs = expressionEvaluator.getUdfs();
          Object[] values = new Object[columnIndexes.length];
          for (int i = 0; i < values.length; i++) {
            if (columnIndexes[i] < 0) {
              values[i] = kudfs[i];
            } else {
              values[i] = genericRowValueTypeEnforcer.enforceFieldType(columnIndexes[i], row
                  .getColumns().get(columnIndexes[i]));
            }
          }
          boolean result = (Boolean) ee.evaluate(values);
          return result;
        } catch (InvocationTargetException e) {
          e.printStackTrace();
        } catch (Exception e) {
          e.printStackTrace();
        }
        throw new RuntimeException("Invalid format.");
      }
    };
  }
}
