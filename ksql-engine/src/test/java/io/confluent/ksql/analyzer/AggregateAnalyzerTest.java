/*
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

package io.confluent.ksql.analyzer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class AggregateAnalyzerTest {

  private static final DereferenceExpression DEFAULT_ARGUMENT = new DereferenceExpression(
      new QualifiedNameReference(QualifiedName.of("ORDERS")), SchemaUtil.ROWTIME_NAME);

  private static final DereferenceExpression COL0 = new DereferenceExpression(
      new QualifiedNameReference(QualifiedName.of("ORDERS")), "COL0");

  private static final DereferenceExpression COL1 = new DereferenceExpression(
      new QualifiedNameReference(QualifiedName.of("ORDERS")), "COL1");

  private static final DereferenceExpression COL2 = new DereferenceExpression(
      new QualifiedNameReference(QualifiedName.of("ORDERS")), "COL2");

  private static final FunctionCall FUNCTION_CALL = new FunctionCall(QualifiedName.of("UCASE"),
      ImmutableList.of(COL0));

  private static final FunctionCall AGG_FUNCTION_CALL = new FunctionCall(QualifiedName.of("MAX"),
      ImmutableList.of(COL0, COL1));

  private MetaStore metaStore;
  private final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private AggregateAnalysis analysis;
  private AggregateAnalyzer analyzer;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(functionRegistry);
    analysis = new AggregateAnalysis();
    analyzer = new AggregateAnalyzer(analysis, DEFAULT_ARGUMENT, functionRegistry);
  }

  @Test
  public void shouldCaptureNonAggregateFunctionArguments() {
    // When:
    analyzer.process(FUNCTION_CALL, false);

    // Then:
    assertThat(analysis.getNonAggregateSelectColumns(), contains(COL0));
    assertThat(analysis.getGroupByColumns(), is(empty()));
    assertThat(analysis.getRequiredColumns(), contains(COL0));
  }

  @Test
  public void shouldCaptureGroupByNonAggregateFunctionArguments() {
    // When:
    analyzer.process(FUNCTION_CALL, true);

    // Then:
    assertThat(analysis.getNonAggregateSelectColumns(), is(empty()));
    assertThat(analysis.getGroupByColumns(), contains(COL0));
    assertThat(analysis.getRequiredColumns(), contains(COL0));
  }

  @Test
  public void shouldNotCaptureNonAggregateFunction() {
    // When:
    analyzer.process(FUNCTION_CALL, false);

    // Then:
    assertThat(analysis.getFunctionList(), is(empty()));
  }

  @Test
  public void shouldCaptureAggregateFunctionArguments() {
    // When:
    analyzer.process(AGG_FUNCTION_CALL, false);

    // Then:
    assertThat(analysis.getNonAggregateSelectColumns(), is(empty()));
    assertThat(analysis.getGroupByColumns(), empty());
    assertThat(analysis.getRequiredColumns(), contains(COL0, COL1));
  }

  @Test
  public void shouldCaptureGroupByAggregateFunctionArguments() {
    // When:
    analyzer.process(AGG_FUNCTION_CALL, true);

    // Then:
    assertThat(analysis.getNonAggregateSelectColumns(), is(empty()));
    assertThat(analysis.getGroupByColumns(), contains(COL0, COL1));
    assertThat(analysis.getRequiredColumns(), contains(COL0, COL1));
  }

  @Test
  public void shouldCaptureAggregateFunction() {
    // When:
    analyzer.process(AGG_FUNCTION_CALL, false);

    // Then:
    assertThat(analysis.getFunctionList(), contains(AGG_FUNCTION_CALL));
  }

  @Test
  public void shouldCaptureNestedFunctions() {
    // Given:
    final FunctionCall nestedFunctionCall = new FunctionCall(QualifiedName.of("MIN"),
        ImmutableList.of(AGG_FUNCTION_CALL, COL2));

    // When:
    analyzer.process(nestedFunctionCall, true);

    // Then:
    assertThat(analysis.getFunctionList(),
        containsInAnyOrder(AGG_FUNCTION_CALL, nestedFunctionCall));
    assertThat(analysis.getNonAggregateSelectColumns(), is(empty()));
    assertThat(analysis.getGroupByColumns(), contains(COL0, COL1, COL2));
    assertThat(analysis.getRequiredColumns(), contains(COL0, COL1, COL2));
  }

  @Test
  public void shouldCaptureDefaultFunctionArguments() {
    // Given:
    final FunctionCall emptyFunc = new FunctionCall(QualifiedName.of("COUNT"), new ArrayList<>());

    // When:
    analyzer.process(emptyFunc, false);

    // Then:
    assertThat(analysis.getFunctionList(), containsInAnyOrder(emptyFunc));
    assertThat(analysis.getRequiredColumns(), contains(DEFAULT_ARGUMENT));
  }

  @Test
  public void shouldAddDefaultArgToFunctionCallWithNoArgs() {
    // Given:
    final FunctionCall emptyFunc = new FunctionCall(QualifiedName.of("COUNT"), new ArrayList<>());

    // When:
    analyzer.process(emptyFunc, false);

    // Then:
    assertThat(emptyFunc.getArguments(), contains(DEFAULT_ARGUMENT));
  }
}
