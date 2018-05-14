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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.MetaStoreFixture;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class SetParentVisitorTest {

  private static final KsqlParser KSQL_PARSER = new KsqlParser();

  private MetaStore metaStore;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore();
  }

  private Statement getAstWithParent(String sql) {
    Statement statement = KSQL_PARSER.buildAst(sql, metaStore).get(0);

    SetParentVisitor setParentVisitor = new SetParentVisitor();
    setParentVisitor.process(statement, null);

    return statement;
  }

  @Test
  public void shouldSetParentsCorrectlyForSimpleQuery() throws Exception {
    Statement statement = getAstWithParent("SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");

    Assert.assertFalse(statement.getParent().isPresent());
    assertThat(statement, instanceOf(Query.class));
    Query query = (Query) statement;
    Assert.assertTrue(query.getQueryBody().getParent().isPresent());
    assertThat(query.getQueryBody().getParent().get(), equalTo(query));
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    Select select = querySpecification.getSelect();
    assertThat(select.getParent().get(), equalTo(querySpecification));

  }

  @Test
  public void shouldSetParentsCorrectlyForQueryWithUDF() throws Exception {
    Statement statement = getAstWithParent("SELECT lcase(col1), concat(col2,'hello'), floor(abs(col3)) FROM test1 t1;");
    Query query = (Query) statement;
    QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    SingleColumn firstSelectItem = (SingleColumn) querySpecification.getSelect().getSelectItems().get(0);
    FunctionCall functionCall = (FunctionCall) firstSelectItem.getExpression();
    assertThat(functionCall.getParent().get(), equalTo(firstSelectItem));
    assertThat(functionCall.getArguments().get(0).getParent().get(), equalTo(functionCall));
  }

  @Test
  public void shouldSetParentsCorrectlyForCreateStreamAsSelect() throws Exception {
    Statement statement = getAstWithParent("CREATE STREAM bigorders_json WITH (value_format = 'json', "
                                           + "kafka_topic='bigorders_topic') AS SELECT * FROM orders WHERE orderunits > 5 ;");

    CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
    Query query = createStreamAsSelect.getQuery();
    QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();

    assertThat(querySpecification.getWhere().get().getParent().get(), equalTo(querySpecification));
    assertThat(querySpecification.getFrom().getParent().get(), equalTo(querySpecification));
  }

}
