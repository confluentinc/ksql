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

import java.util.Objects;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Windowed;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.codegen.CodeGenRunner;
import io.confluent.ksql.codegen.SqlToJavaVisitor;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;

public class SqlPredicate {
  private static final Logger log = LoggerFactory.getLogger(SqlPredicate.class);

  private final Expression filterExpression;
  private final Schema schema;
  private final IExpressionEvaluator ee;
  private final int[] columnIndexes;
  private final boolean isWindowedKey;
  private final KsqlConfig ksqlConfig;
  private final FunctionRegistry functionRegistry;
  private final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer;

  SqlPredicate(
      final Expression filterExpression,
      final Schema schema,
      final boolean isWindowedKey,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    this.filterExpression = filterExpression;
    this.schema = schema;
    this.genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schema);
    this.isWindowedKey = isWindowedKey;
    this.functionRegistry = functionRegistry;
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");

    final CodeGenRunner codeGenRunner = new CodeGenRunner(schema, ksqlConfig, functionRegistry);
    final Set<CodeGenRunner.ParameterType> parameters
        = codeGenRunner.getParameterInfo(filterExpression);

    final String[] parameterNames = new String[parameters.size()];
    final Class[] parameterTypes = new Class[parameters.size()];
    columnIndexes = new int[parameters.size()];
    int index = 0;
    for (final CodeGenRunner.ParameterType param : parameters) {
      parameterNames[index] = param.getName();
      parameterTypes[index] = param.getType();
      columnIndexes[index] = SchemaUtil.getFieldIndexByName(schema, param.getName());
      index++;
    }

    try {
      ee = CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();
      ee.setDefaultImports(CodeGenRunner.CODEGEN_IMPORTS.toArray(new String[0]));
      ee.setParameters(parameterNames, parameterTypes);

      ee.setExpressionType(boolean.class);

      String expressionStr = new SqlToJavaVisitor(
          schema,
          functionRegistry
      ).process(filterExpression);

      ee.cook(expressionStr);
    } catch (Exception e) {
      throw new KsqlException(
          "Failed to generate code for SqlPredicate."
          + "filterExpression: "
          + filterExpression
          + "schema:"
          + schema
          + "isWindowedKey:"
          + isWindowedKey,
          e
      );
    }
  }

  Predicate getPredicate() {
    if (isWindowedKey) {
      return getWindowedKeyPredicate();
    } else {
      return getStringKeyPredicate();
    }
  }

  private Predicate<String, GenericRow> getStringKeyPredicate() {
    final ExpressionMetadata expressionEvaluator = createExpressionMetadata();

    return (key, row) -> {
      if (row == null) {
        return false;
      }
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
        return (Boolean) ee.evaluate(values);
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
      log.error("Invalid format: " + key + " : " + row);
      return false;
    };
  }

  private ExpressionMetadata createExpressionMetadata() {
    final CodeGenRunner codeGenRunner = new CodeGenRunner(schema, ksqlConfig, functionRegistry);
    try {
      return codeGenRunner.buildCodeGenFromParseTree(filterExpression);
    } catch (Exception e) {
      throw new KsqlException(
          "Failed to generate code for filterExpression:"
          + filterExpression
          + " schema:"
          + schema,
          e
      );
    }
  }

  private Predicate getWindowedKeyPredicate() {
    final ExpressionMetadata expressionEvaluator = createExpressionMetadata();
    return (Predicate<Windowed<String>, GenericRow>) (key, row) -> {
      if (row == null) {
        return false;
      }
      try {
        Kudf[] kudfs = expressionEvaluator.getUdfs();
        Object[] values = new Object[columnIndexes.length];
        for (int i = 0; i < values.length; i++) {
          if (columnIndexes[i] < 0) {
            values[i] = kudfs[i];
          } else {
            values[i] = genericRowValueTypeEnforcer
                .enforceFieldType(
                    columnIndexes[i],
                    row.getColumns().get(columnIndexes[i]
                    )
                );
          }
        }
        return (Boolean) ee.evaluate(values);
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

  // visible for testing
  int[] getColumnIndexes() {
    // As this is only used for testing it is ok to do the array copy.
    // We need to revisit the tests for this class and remove this.
    final int[] result = new int[columnIndexes.length];
    System.arraycopy(columnIndexes, 0, result, 0, columnIndexes.length);
    return result;
  }

}
