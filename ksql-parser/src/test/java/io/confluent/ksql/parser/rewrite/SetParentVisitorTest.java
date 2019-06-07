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

package io.confluent.ksql.parser.rewrite;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.MetaStoreFixture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SetParentVisitorTest {

  private static final KsqlParser KSQL_PARSER = new KsqlParser();

  private MetaStore metaStore;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(new TestFunctionRegistry());
  }

  private Statement getAstWithParent(final String sql) {
    final Statement statement = KSQL_PARSER.buildAst(sql, metaStore).get(0);

    final SetParentVisitor setParentVisitor = new SetParentVisitor();
    setParentVisitor.process(statement, null);

    return statement;
  }

  @Test
  public void shouldSetParentsCorrectlyForSimpleQuery() {
    final Statement statement = getAstWithParent("SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");

    Assert.assertFalse(statement.getParent().isPresent());
    assertThat(statement, instanceOf(Query.class));
    final Query query = (Query) statement;
    Assert.assertTrue(query.getQueryBody().getParent().isPresent());
    assertThat(query.getQueryBody().getParent().get(), equalTo(query));
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    final Select select = querySpecification.getSelect();
    assertThat(select.getParent().get(), equalTo(querySpecification));

  }

  @Test
  public void shouldSetParentsCorrectlyForQueryWithUDF() {
    final Statement statement = getAstWithParent("SELECT lcase(col1), concat(col2,'hello'), floor(abs(col3)) FROM test1 t1;");
    final Query query = (Query) statement;
    final QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    final SingleColumn firstSelectItem = (SingleColumn) querySpecification.getSelect().getSelectItems().get(0);
    final FunctionCall functionCall = (FunctionCall) firstSelectItem.getExpression();
    assertThat(functionCall.getParent().get(), equalTo(firstSelectItem));
    assertThat(functionCall.getArguments().get(0).getParent().get(), equalTo(functionCall));
  }

  @Test
  public void shouldSetParentsCorrectlyForCreateStreamAsSelect() {
    final Statement statement = getAstWithParent("CREATE STREAM bigorders_json WITH (value_format = 'json', "
                                           + "kafka_topic='bigorders_topic') AS SELECT * FROM orders WHERE orderunits > 5 ;");

    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
    final Query query = createStreamAsSelect.getQuery();
    final QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();

    assertThat(querySpecification.getWhere().get().getParent().get(), equalTo(querySpecification));
    assertThat(querySpecification.getFrom().getParent().get(), equalTo(querySpecification));
  }

}
