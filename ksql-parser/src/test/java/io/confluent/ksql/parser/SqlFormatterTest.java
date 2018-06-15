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

package io.confluent.ksql.parser;

import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.function.TestFunctionRegistry;

import org.junit.Before;
import org.junit.Test;

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.tree.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SqlFormatterTest {

  Table left;
  Table right;
  AliasedRelation leftAlias;
  AliasedRelation rightAlias;
  JoinCriteria criteria;
  NodeLocation location;

  @Before
  public void setUp() {
    left = new Table(QualifiedName.of(Collections.singletonList("left")));
    right = new Table(QualifiedName.of(Collections.singletonList("right")));
    leftAlias = new AliasedRelation(left, "l", Collections.emptyList());
    rightAlias = new AliasedRelation(right, "r", Collections.emptyList());

    criteria = new JoinOn(new ComparisonExpression(ComparisonExpression.Type.EQUAL,
                                                   new StringLiteral("left.col0"),
                                                   new StringLiteral("right.col0")));
    location = new NodeLocation(0, 0);
  }

  @Test
  public void testFormatSql() {

    ArrayList<TableElement> tableElements = new ArrayList<>();
    tableElements.add(new TableElement("GROUP", new PrimitiveType(Type.KsqlType.STRING)));
    tableElements.add(new TableElement("NOLIT", new PrimitiveType(Type.KsqlType.STRING)));
    tableElements.add(new TableElement("Having", new PrimitiveType(Type.KsqlType.STRING)));

    CreateStream createStream = new CreateStream(
        QualifiedName.of("TEST"),
        tableElements,
        false,
        Collections.singletonMap(
            DdlConfig.TOPIC_NAME_PROPERTY,
            new StringLiteral("topic_test")
        ));
    String sql = SqlFormatter.formatSql(createStream);
    assertThat("literal escaping failure", sql, containsString("`GROUP` STRING"));
    assertThat("not literal escaping failure", sql, containsString("NOLIT STRING"));
    assertThat("lowercase literal escaping failure", sql, containsString("`Having` STRING"));
    List<Statement> statements = new KsqlParser().buildAst(sql,
        MetaStoreFixture.getNewMetaStore(new TestFunctionRegistry()));
    assertFalse("formatted sql parsing error", statements.isEmpty());
  }


  @Test
  public void shouldFormatLeftJoinWithSpan() {
    final Join join = new Join(location, Join.Type.LEFT, leftAlias, rightAlias,
                         Optional.of(criteria),
                         Optional.of(new SpanExpression(10, TimeUnit.SECONDS)));

    final String expected = "left L\nLEFT OUTER JOIN right R ON (('left.col0' = 'right.col0')) "
                           + "SPAN 10 SECONDS";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }

  @Test
  public void shouldFormatLeftJoinWithoutJoinWindow() {
    final Join join = new Join(location, Join.Type.LEFT, leftAlias, rightAlias,
                               Optional.of(criteria), Optional.empty());

    final String result = SqlFormatter.formatSql(join);
    final String expected = "left L\nLEFT OUTER JOIN right R ON (('left.col0' = 'right.col0'))";
    assertEquals(expected, result);
  }

  @Test
  public void shouldFormatInnerJoin() {
    final Join join = new Join(location, Join.Type.INNER, leftAlias, rightAlias,
                               Optional.of(criteria),
                               Optional.of(new SpanExpression(10, TimeUnit.SECONDS)));

    final String expected = "left L\nINNER JOIN right R ON (('left.col0' = 'right.col0')) "
                            + "SPAN 10 SECONDS";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }

  @Test
  public void shouldFormatInnerJoinWithoutJoinWindow() {
    final Join join = new Join(location, Join.Type.INNER, leftAlias, rightAlias,
                               Optional.of(criteria),
                               Optional.empty());

    final String expected = "left L\nINNER JOIN right R ON (('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }


  @Test
  public void shouldFormatOuterJoin() {
    final Join join = new Join(location, Join.Type.OUTER, leftAlias, rightAlias,
                               Optional.of(criteria),
                               Optional.of(new SpanExpression(10, TimeUnit.SECONDS)));

    final String expected = "left L\nFULL OUTER JOIN right R ON (('left.col0' = 'right.col0')) "
                            + "SPAN 10 SECONDS";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }


  @Test
  public void shouldFormatOuterJoinWithoutJoinWindow() {
    final Join join = new Join(location, Join.Type.OUTER, leftAlias, rightAlias,
                               Optional.of(criteria),
                               Optional.empty());

    final String expected = "left L\nFULL OUTER JOIN right R ON (('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }
}

