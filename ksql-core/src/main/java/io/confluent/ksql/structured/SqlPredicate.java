/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.structured;

import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.util.ExpressionUtil;
import io.confluent.ksql.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.util.SchemaUtil;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Windowed;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class SqlPredicate {

  Expression filterExpression;
  final Schema schema;
  IExpressionEvaluator ee;
  int[] columnIndexes;
  boolean isWindowedKey;

  GenericRowValueTypeEnforcer genericRowValueTypeEnforcer;
  private static final Logger log = LoggerFactory.getLogger(SqlPredicate.class);

  public SqlPredicate(final Expression filterExpression, final Schema schema,
                      boolean isWindowedKey) throws Exception {
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

    ee = CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();

    // The expression will have two "int" parameters: "a" and "b".
    ee.setParameters(parameterNames, parameterTypes);

    // And the expression (i.e. "result") type is also "int".
    ee.setExpressionType(boolean.class);

    String expressionStr = filterExpression.getCodegenString(schema);

    // And now we "cook" (scan, parse, compile and load) the fabulous expression.
    ee.cook(expressionStr);
  }

  public Predicate getPredicate() throws Exception {
    if (isWindowedKey) {
      return getWindowedKeyPredicate();
    } else {
      return getStringKeyPredicate();
    }
  }

  private Predicate getStringKeyPredicate() throws Exception {
    ExpressionUtil expressionUtil = new ExpressionUtil();
    ExpressionMetadata
        expressionEvaluator =
        expressionUtil.getExpressionEvaluator(filterExpression, schema);
    return new Predicate<String, GenericRow>() {
      @Override
      public boolean test(String key, GenericRow row) {
        try {
          Kudf[] kudfs = expressionEvaluator.getUdfs();
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
        } catch (Exception e) {
          log.error(e.getMessage(), e);
        }
        log.error("Invalid format: " + key + " : " + row);
        return false;
      }
    };
  }

  private Predicate getWindowedKeyPredicate() throws Exception {
    ExpressionUtil expressionUtil = new ExpressionUtil();
    ExpressionMetadata
        expressionEvaluator =
        expressionUtil.getExpressionEvaluator(filterExpression, schema);
    return new Predicate<Windowed<String>, GenericRow>() {
      @Override
      public boolean test(Windowed<String> key, GenericRow row) {
        try {
          Kudf[] kudfs = expressionEvaluator.getUdfs();
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
        } catch (Exception e) {
          log.error(e.getMessage(), e);
        }
        log.error("Invalid format: " + key + " : " + row);
        return false;
      }
    };
  }
}
