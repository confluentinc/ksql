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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;

import io.confluent.common.logging.StructuredLogger;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.processing.log.ProcessingLogContext;
import io.confluent.ksql.processing.log.ProcessingLogMessageSchema;
import io.confluent.ksql.processing.log.ProcessingLogMessageSchema.MessageType;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.Collections;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class SqlPredicateTest {
  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());

  private MetaStore metaStore;
  private InternalFunctionRegistry functionRegistry;
  private ProcessingLogContext processingLogContext;

  @Mock
  private StructuredLogger processingLogger;

  @Captor
  private ArgumentCaptor<Supplier<SchemaAndValue>> schemaAndValueCaptor;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
    functionRegistry = new InternalFunctionRegistry();
    processingLogContext = ProcessingLogContext.create();
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
    final SqlPredicate predicate = givenSqlPredicateFor(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 AND LEN(col2) = 5;");

    // When/Then:
    assertThat(
        predicate.getFilterExpression().toString().toUpperCase(),
        equalTo("((TEST1.COL0 > 100) AND (LEN(TEST1.COL2) = 5))"));
    assertThat(predicate.getColumnIndexes().length, equalTo(3));
  }

  @Test
  public void shouldIgnoreNullRows() {
    // Given:
    final SqlPredicate sqlPredicate = givenSqlPredicateFor(
        "SELECT col0 FROM test1 WHERE col0 > 100;");

    // When/Then:
    assertThat(sqlPredicate.getPredicate().test("key", null), is(false));
  }

  @Test
  public void shouldWriteProcessingLogOnError() {
    // Given:
    final SqlPredicate sqlPredicate = givenSqlPredicateFor(
        "SELECT col0 FROM test1 WHERE col0 > 100;");

    // When:
    sqlPredicate.getPredicate().test(
        "key",
        new GenericRow(0L, "key", Collections.emptyList()));

    // Then:
    verify(processingLogger).error(schemaAndValueCaptor.capture());
    final SchemaAndValue schemaAndValue = schemaAndValueCaptor.getValue().get();
    assertThat(schemaAndValue.schema(), equalTo(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA));
    final Struct struct = (Struct) schemaAndValue.value();
    assertThat(
        struct.get(ProcessingLogMessageSchema.TYPE),
        equalTo(MessageType.RECORD_PROCESSING_ERROR.ordinal()));
    assertThat(
        struct.getStruct(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR)
            .get(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_MESSAGE),
        equalTo(
            "Error evaluating predicate (TEST1.COL0 > 100): "
                + "Invalid field type. Value must be Long.")
    );
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
        processingLogger,
        processingLogContext);
  }

  private PlanNode buildLogicalPlan(final String queryStr) {
    return AnalysisTestUtil.buildLogicalPlan(queryStr, metaStore);
  }
}
