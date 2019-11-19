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

package io.confluent.ksql.integration;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.transform.KsqlValueTransformerWithKey;
import io.confluent.ksql.execution.transform.SelectValueMapperFactory;
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class SelectValueMapperIntegrationTest {

  private static final Struct NON_WINDOWED_KEY = StructKeyUtil.asStructKey("someKey");

  private final MetaStore metaStore = MetaStoreFixture
      .getNewMetaStore(new InternalFunctionRegistry());

  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());

  @Mock
  private ProcessingLogger processingLogger;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Test
  public void shouldSelectChosenColumns() {
    // Given:
    final KsqlValueTransformerWithKey<Struct> selectTransformer = givenSelectMapperFor(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;");

    // When:
    final GenericRow transformed = selectTransformer.transform(
        NON_WINDOWED_KEY,
        genericRow(1521834663L, "key1", 1L, "hi", "bye", 2.0D, "blah")
    );

    // Then:
    assertThat(transformed, is(genericRow(1L, "bye", 2.0D)));
  }

  @Test
  public void shouldApplyUdfsToColumns() {
    // Given:
    final KsqlValueTransformerWithKey<Struct> selectTransformer = givenSelectMapperFor(
        "SELECT col0, col1, col2, CEIL(col3) FROM test1 WHERE col0 > 100 EMIT CHANGES;");

    // When:
    final GenericRow row = selectTransformer.transform(
        NON_WINDOWED_KEY,
        genericRow(1521834663L, "key1", 2L, "foo", "whatever", 6.9D, "boo", "hoo")
    );

    // Then:
    assertThat(row, is(genericRow(2L, "foo", "whatever", 7.0D)));
  }

  private KsqlValueTransformerWithKey<Struct> givenSelectMapperFor(final String query) {
    final PlanNode planNode = AnalysisTestUtil.buildLogicalPlan(ksqlConfig, query, metaStore);
    final ProjectNode projectNode = (ProjectNode) planNode.getSources().get(0);
    final LogicalSchema schema = planNode.getTheSourceNode().getSchema();
    final List<SelectExpression> selectExpressions = projectNode.getSelectExpressions();

    return SelectValueMapperFactory.<Struct>create(
        selectExpressions,
        schema,
        ksqlConfig,
        new InternalFunctionRegistry()
    ).getTransformer(processingLogger);
  }

  private static GenericRow genericRow(final Object... columns) {
    return new GenericRow(Arrays.asList(columns));
  }
}
