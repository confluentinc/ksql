/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.structured;

import static io.confluent.ksql.testutils.AnalysisTestUtil.analyzeQuery;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.common.logging.StructuredLogger;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.analyzer.AggregateAnalysis;
import io.confluent.ksql.analyzer.AggregateAnalyzer;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.processing.log.ProcessingLogMessageSchema;
import io.confluent.ksql.processing.log.ProcessingLogMessageSchema.MessageType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@SuppressWarnings("unchecked")
public class SqlPredicateTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());

  private MetaStore metaStore;
  private InternalFunctionRegistry functionRegistry;

  @Mock
  private StructuredLogger processingLogger;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
    functionRegistry = new InternalFunctionRegistry();
  }

  private PlanNode buildLogicalPlan(final String queryStr) {
    final Analysis analysis = analyzeQuery(queryStr, metaStore);
    final AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
    final AggregateAnalyzer aggregateAnalyzer = new AggregateAnalyzer(aggregateAnalysis,
                                                                analysis, functionRegistry);
    for (final Expression expression: analysis.getSelectExpressions()) {
      aggregateAnalyzer.process(expression, new AnalysisContext(null));
    }
    // Build a logical plan
    return new LogicalPlanner(analysis, aggregateAnalysis, functionRegistry).buildPlan();
  }

  @Test
  public void testFilter() {
    // Given:
    final SqlPredicate predicate = givenSqlPredicateFor(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");

    // When/Then:
    assertThat(
        predicate.getFilterExpression().toString().toUpperCase(),
        equalTo("(TEST1.COL0 > 100)"));
    assertThat(predicate.getColumnIndexes().length, equalTo(1));

  }

  @Test
  public void testFilterBiggerExpression() {
    // Given:
    final SqlPredicate predicate =
        givenSqlPredicateFor(
            "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 AND LEN(col2) = 5;");

    // When/Then:
    assertThat(
        predicate.getFilterExpression().toString().toUpperCase(),
        equalTo("((TEST1.COL0 > 100) AND (LEN(TEST1.COL2) = 5))"));
    assertThat(predicate.getColumnIndexes().length, equalTo(3));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldIgnoreNullRows() {
    // Given:
    final SqlPredicate sqlPredicate =
        givenSqlPredicateFor("SELECT col0 FROM test1 WHERE col0 > 100;");

    // When/Then:
    assertThat(sqlPredicate.getPredicate().test("key", null), is(false));
  }

  @Test
  public void shouldWriteProcessingLogOnError() throws IOException {
    // Given:
    final SqlPredicate sqlPredicate =
        givenSqlPredicateFor("SELECT col0 FROM test1 WHERE col0 > 100;");

    // When:
    sqlPredicate.getPredicate().test(
        "key",
        new GenericRow(0L, "key", Collections.emptyList()));

    // Then:
    final ArgumentCaptor<Supplier<SchemaAndValue>> captor
        = ArgumentCaptor.forClass(Supplier.class);
    verify(processingLogger).error(captor.capture());
    final SchemaAndValue schemaAndValue = captor.getValue().get();
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
            "Error evaluating predicate (TEST1.COL0 > 100): "
                + "Invalid field type. Value must be Long.")
    );
    final String rowString =
        errorStruct.getString(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_RECORD);
    final List<Object> row = (List) MAPPER.readValue(rowString, List.class);
    assertThat(row, Matchers.contains(0, "key", Collections.emptyList()));
  }

  private SqlPredicate givenSqlPredicateFor(final String statement) {
    final PlanNode logicalPlan = buildLogicalPlan(statement);
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);
    return new SqlPredicate(
        filterNode.getPredicate(),
        logicalPlan.getTheSourceNode().getSchema(),
        false,
        ksqlConfig,
        functionRegistry,
        processingLogger);
  }
}
