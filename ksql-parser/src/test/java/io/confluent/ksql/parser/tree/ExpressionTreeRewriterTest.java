/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.parser.tree;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.mock;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class ExpressionTreeRewriterTest {

  private static final DereferenceExpression DEREF_0
    = new DereferenceExpression(new QualifiedNameReference(
      QualifiedName.of("Bob")), "f0");

  private static final DereferenceExpression DEREF_1
    = new DereferenceExpression(new QualifiedNameReference(
      QualifiedName.of("Jane")), "f1");

  private static final DereferenceExpression DEREF_2
    = new DereferenceExpression(new QualifiedNameReference(
      QualifiedName.of("Vic")), "f2");

  @Mock(MockType.NICE)
  private ExpressionRewriter<String> rewriter;

  @Test
  public void shouldRewriteFunctionCall() {
    // Given:
    final FunctionCall original = givenFunctionCall();
    final FunctionCall expected = mock(FunctionCall.class);

    EasyMock.expect(rewriter.rewriteFunctionCall(eq(original), anyObject(), anyObject()))
        .andReturn(expected);

    EasyMock.replay(rewriter);

    // When:
    final FunctionCall result = ExpressionTreeRewriter.rewriteWith(rewriter, original);

    // Then:
    assertThat(result, is(expected));
  }

  @Test
  public void shouldRewriteFunctionCallArguments() {
    // Given:
    final FunctionCall original = givenFunctionCall();

    EasyMock.expect(rewriter.rewriteFunctionCall(eq(original), anyObject(), anyObject()))
        .andReturn(null);

    EasyMock.expect(rewriter.rewriteDereferenceExpression(eq(DEREF_0), anyObject(), anyObject()))
        .andReturn(DEREF_2);


    EasyMock.replay(rewriter);

    // When:
    final FunctionCall result = ExpressionTreeRewriter.rewriteWith(rewriter, original);

    // Then:
    assertThat(result.getName(), is(original.getName()));
    assertThat(result.getWindow(), is(original.getWindow()));
    assertThat(result.getLocation(), is(original.getLocation()));
    assertThat(result.isDistinct(), is(original.isDistinct()));
    assertThat(result.getArguments(), is(ImmutableList.of(DEREF_2, DEREF_1)));
  }

  private FunctionCall givenFunctionCall() {
    final Optional<NodeLocation> location = Optional.of(new NodeLocation(42, 6));
    final QualifiedName name = QualifiedName.of("bob");
    final Optional<Window> window = Optional.of(mock(Window.class));
    final List<Expression> args = ImmutableList.of(DEREF_0, DEREF_1);
    return new FunctionCall(location, name, window, true, args);
  }
}