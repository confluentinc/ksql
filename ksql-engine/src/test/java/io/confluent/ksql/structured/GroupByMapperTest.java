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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.util.ExpressionMetadata;
import java.util.Collections;
import org.apache.kafka.connect.data.Struct;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class GroupByMapperTest {

  @Mock(MockType.NICE)
  private ExpressionMetadata groupBy0;

  @Mock(MockType.NICE)
  private ExpressionMetadata groupBy1;

  @Mock(MockType.NICE)
  private GenericRow row;

  private GroupByMapper mapper;

  @Before
  public void setUp() {
    mapper = new GroupByMapper(ImmutableList.of(groupBy0, groupBy1));
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowOnNullParam() {
    new GroupByMapper(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnEmptyParam() {
    new GroupByMapper(Collections.emptyList());
  }

  @Test
  public void shouldGenerateGroupByKey() {
    // Given:
    EasyMock.expect(groupBy0.evaluate(row)).andReturn("result0");
    EasyMock.expect(groupBy1.evaluate(row)).andReturn("result1");
    EasyMock.replay(groupBy0, groupBy1);

    // When:
    final Struct result = mapper.apply("key", row);

    // Then:
    assertThat(result, is(StructKeyUtil.asStructKey("result0|+|result1")));
  }

  @Test
  public void shouldSupportNullValues() {
    // Given:
    EasyMock.expect(groupBy0.evaluate(row)).andReturn(null);
    EasyMock.expect(groupBy1.evaluate(row)).andReturn("result1");
    EasyMock.replay(groupBy0, groupBy1);

    // When:
    final Struct result = mapper.apply("key", row);

    // Then:
    assertThat(result, is(StructKeyUtil.asStructKey("null|+|result1")));
  }

  @Test
  public void shouldUseNullIfExpressionThrows() {
    // Given:
    EasyMock.expect(groupBy0.evaluate(row)).andThrow(new RuntimeException("Boom"));
    EasyMock.expect(groupBy1.evaluate(row)).andReturn("result1");
    EasyMock.replay(groupBy0, groupBy1);

    // When:
    final Struct result = mapper.apply("key", row);

    // Then:
    assertThat(result, is(StructKeyUtil.asStructKey("null|+|result1")));
  }

  @Test
  public void shouldGetKeyName() {
    // Given:
    final Expression exp0 = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of("Fred")), "f1");
    final Expression exp1 = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of("Bob")), "b1");

    // When:
    final String result = GroupByMapper.keyNameFor(ImmutableList.of(exp0, exp1));

    // Then:
    assertThat(result, is("Fred.f1|+|Bob.b1"));
  }
}