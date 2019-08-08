/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.structured;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.codegen.CodeGenRunner;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.MessageType;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.SelectExpression;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class SelectValueMapperTest {
  private final MetaStore metaStore =
      MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());

  @Mock
  private ProcessingLogger processingLogger;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Test
  public void shouldSelectChosenColumns() {
    // Given:
    final SelectValueMapper selectMapper = givenSelectMapperFor(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");

    // When:
    final GenericRow transformed = selectMapper.apply(
        genericRow(1521834663L, "key1", 1L, "hi", "bye", 2.0F, "blah"));

    // Then:
    assertThat(transformed, is(genericRow(1L, "bye", 2.0F)));
  }

  @Test
  public void shouldApplyUdfsToColumns() {
    // Given:
    final SelectValueMapper selectMapper = givenSelectMapperFor(
        "SELECT col0, col1, col2, CEIL(col3) FROM test1 WHERE col0 > 100;");

    // When:
    final GenericRow row = selectMapper.apply(
        genericRow(1521834663L, "key1", 2L, "foo", "whatever", 6.9F, "boo", "hoo"));

    // Then:
    assertThat(row, is(genericRow(2L, "foo", "whatever", 7.0F)));
  }

  @Test
  public void shouldHandleNullRows() {
    // Given:
    final SelectValueMapper selectMapper = givenSelectMapperFor(
        "SELECT col0, col1, col2, CEIL(col3) FROM test1 WHERE col0 > 100;");

    // When:
    final GenericRow row = selectMapper.apply(null);

    // Then:
    assertThat(row, is(nullValue()));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldWriteProcessingLogOnError() {
    // Given:
    final SelectValueMapper selectMapper = givenSelectMapperFor(
        "SELECT col0, col1, col2, CEIL(col3) FROM test1 WHERE col0 > 100;");

    // When:
    selectMapper.apply(
        new GenericRow(0L, "key", 2L, "foo", "whatever", null, "boo", "hoo"));

    // Then:
    final ArgumentCaptor<Function<ProcessingLogConfig, SchemaAndValue>> captor
        = ArgumentCaptor.forClass(Function.class);
    verify(processingLogger).error(captor.capture());
    final SchemaAndValue schemaAndValue = captor.getValue().apply(
        new ProcessingLogConfig(Collections.emptyMap()));
    assertThat(schemaAndValue.schema(), equalTo(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA));
    final Struct struct = (Struct) schemaAndValue.value();
    assertThat(
        struct.get(ProcessingLogMessageSchema.TYPE),
        equalTo(MessageType.RECORD_PROCESSING_ERROR.ordinal()));
    final Struct errorStruct
        = struct.getStruct(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR);
    assertThat(
        errorStruct.get(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_MESSAGE),
        equalTo(
            "Error computing expression CEIL(TEST1.COL3) "
                + "for column KSQL_COL_3 with index 3: null")
    );
  }

  private SelectValueMapper givenSelectMapperFor(final String query) {
    final PlanNode planNode = AnalysisTestUtil.buildLogicalPlan(ksqlConfig, query, metaStore);
    final ProjectNode projectNode = (ProjectNode) planNode.getSources().get(0);
    final LogicalSchema schema = planNode.getTheSourceNode().getSchema();
    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();
    final List<ExpressionMetadata> metadata = createExpressionMetadata(selectExpressions, schema);
    final List<String> selectFieldNames = selectExpressions.stream()
        .map(SelectExpression::getName)
        .collect(Collectors.toList());
    return new SelectValueMapper(
        selectFieldNames,
        metadata,
        processingLogger
    );
  }

  private List<ExpressionMetadata> createExpressionMetadata(
      final List<SelectExpression> selectExpressions,
      final LogicalSchema schema
  ) {
    try {
      final CodeGenRunner codeGenRunner = new CodeGenRunner(
          schema, ksqlConfig, new InternalFunctionRegistry());

      final List<ExpressionMetadata> expressionEvaluators = new ArrayList<>();
      for (final SelectExpression expressionPair : selectExpressions) {
        expressionEvaluators
            .add(codeGenRunner.buildCodeGenFromParseTree(expressionPair.getExpression(), "Select"));
      }
      return expressionEvaluators;
    } catch (final Exception e) {
      throw new AssertionError("Invalid test", e);
    }
  }

  private static GenericRow genericRow(final Object... columns) {
    return new GenericRow(Arrays.asList(columns));
  }
}
