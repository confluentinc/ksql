/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser.rewrite;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParserTestUtil;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CustomTypeRewriterTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private MetaStore metaStore;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(mock(FunctionRegistry.class));
  }

  @Test
  public void shouldRewriteCustomTypes() {
    // Given:
    metaStore.registerType("MY_TYPE", SqlPrimitiveType.of(SqlBaseType.STRING));
    final String stmtString = "CREATE STREAM foo (f1 MY_TYPE) WITH(kafka_topic='foo', value_format='AVRO');";

    // When:
    final Statement stmt = KsqlParserTestUtil.buildSingleAst(stmtString, metaStore).getStatement();

    // Then:
    assertThat(
        SqlFormatter.formatSql(stmt),
        is("CREATE STREAM FOO (F1 STRING) WITH (KAFKA_TOPIC='foo', VALUE_FORMAT='AVRO');"));
  }

  @Test
  public void shouldRewriteArraysOfCustomTypes() {
    // Given:
    metaStore.registerType("MY_TYPE", SqlPrimitiveType.of(SqlBaseType.STRING));
    final String stmtString = "CREATE STREAM foo (f1 ARRAY<MY_TYPE>) WITH(kafka_topic='foo', value_format='AVRO');";

    // When:
    final Statement stmt = KsqlParserTestUtil.buildSingleAst(stmtString, metaStore).getStatement();

    // Then:
    assertThat(
        SqlFormatter.formatSql(stmt),
        is("CREATE STREAM FOO (F1 ARRAY<STRING>) WITH (KAFKA_TOPIC='foo', VALUE_FORMAT='AVRO');"));
  }

  @Test
  public void shouldRewriteMapsOfCustomTypes() {
    // Given:
    metaStore.registerType("MY_TYPE", SqlPrimitiveType.of(SqlBaseType.STRING));
    final String stmtString = "CREATE STREAM foo (f1 MAP<STRING, MY_TYPE>) WITH(kafka_topic='foo', value_format='AVRO');";

    // When:
    final Statement stmt = KsqlParserTestUtil.buildSingleAst(stmtString, metaStore).getStatement();

    // Then:
    assertThat(
        SqlFormatter.formatSql(stmt),
        is("CREATE STREAM FOO (F1 MAP<STRING, STRING>) WITH (KAFKA_TOPIC='foo', VALUE_FORMAT='AVRO');"));
  }

  @Test
  public void shouldRewriteStructsOfCustomTypes() {
    // Given:
    metaStore.registerType("MY_TYPE", SqlPrimitiveType.of(SqlBaseType.STRING));
    final String stmtString = "CREATE STREAM foo (f1 STRUCT<bar MY_TYPE>) WITH(kafka_topic='foo', value_format='AVRO');";

    // When:
    final Statement stmt = KsqlParserTestUtil.buildSingleAst(stmtString, metaStore).getStatement();

    // Then:
    assertThat(
        SqlFormatter.formatSql(stmt),
        is("CREATE STREAM FOO (F1 STRUCT<BAR STRING>) WITH (KAFKA_TOPIC='foo', VALUE_FORMAT='AVRO');"));
  }

  @Test
  public void shouldHandleBaseTypes() {
    // Given:
    final String stmtString = "CREATE STREAM foo (f1 STRING) WITH(kafka_topic='foo', value_format='AVRO');";

    // When:
    final Statement stmt = KsqlParserTestUtil.buildSingleAst(stmtString, metaStore).getStatement();

    // Then:
    assertThat(
        SqlFormatter.formatSql(stmt),
        is("CREATE STREAM FOO (F1 STRING) WITH (KAFKA_TOPIC='foo', VALUE_FORMAT='AVRO');"));
  }

  @Test
  public void shouldThrowIfNoCustomType() {
    // Given:
    final String stmtString = "CREATE STREAM foo (f1 MY_TYPE) WITH(kafka_topic='foo', value_format='AVRO');";

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Cannot resolve unknown type or alias: MY_TYPE");

    // When:
    KsqlParserTestUtil.buildSingleAst(stmtString, metaStore);
  }

}