/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.structured;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.codegen.CodeGenRunner;
import io.confluent.ksql.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.codegen.SqlToJavaVisitor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Windowed;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SqlPredicate {

  private Expression filterExpression;
  private final Schema schema;
  private IExpressionEvaluator ee;
  private int[] columnIndexes;
  private boolean isWindowedKey;
  private final FunctionRegistry functionRegistry;

  private GenericRowValueTypeEnforcer genericRowValueTypeEnforcer;
  private static final Logger log = LoggerFactory.getLogger(SqlPredicate.class);

  SqlPredicate(final Expression filterExpression,
               final Schema schema,
               boolean isWindowedKey,
               final FunctionRegistry functionRegistry) {
    this.filterExpression = filterExpression;
    this.schema = schema;
    this.genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schema);
    this.isWindowedKey = isWindowedKey;
    this.functionRegistry = functionRegistry;

    CodeGenRunner codeGenRunner = new CodeGenRunner(schema, functionRegistry);
    Map<String, Class> parameterMap = codeGenRunner.getParameterInfo(filterExpression);

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

    try {
      ee = CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();

      // The expression will have two "int" parameters: "a" and "b".
      ee.setParameters(parameterNames, parameterTypes);

      // And the expression (i.e. "result") type is also "int".
      ee.setExpressionType(boolean.class);

      String expressionStr = new SqlToJavaVisitor(schema, functionRegistry).process(filterExpression);

      // And now we "cook" (scan, parse, compile and load) the fabulous expression.
      ee.cook(expressionStr);
    } catch (Exception e) {
      throw new KsqlException("Failed to generate code for SqlPredicate."
          + "filterExpression: "
          + filterExpression
          + "schema:"
          + schema
          + "isWindowedKey:"
          + isWindowedKey,
          e);
    }
  }

  Predicate getPredicate() {
    if (isWindowedKey) {
      return getWindowedKeyPredicate();
    } else {
      return getStringKeyPredicate();
    }
  }

  private Predicate<String, GenericRow> getStringKeyPredicate()  {
    final ExpressionMetadata expressionEvaluator = createExpressionMetadata();

    return (key, row) -> {
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
    };
  }

  private ExpressionMetadata createExpressionMetadata() {
    final CodeGenRunner codeGenRunner = new CodeGenRunner(schema, functionRegistry);
    try {
      return codeGenRunner.buildCodeGenFromParseTree(filterExpression);
    } catch (Exception e) {
      throw new KsqlException("Failed to generate code for filterExpression:" + filterExpression + " schema:" + schema, e);
    }
  }

  private Predicate getWindowedKeyPredicate()  {
    final ExpressionMetadata expressionEvaluator = createExpressionMetadata();
    return (Predicate<Windowed<String>, GenericRow>) (key, row) -> {
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
    };
  }

  public Expression getFilterExpression() {
    return filterExpression;
  }

  public Schema getSchema() {
    return schema;
  }

  public int[] getColumnIndexes() {
    return columnIndexes;
  }

}
