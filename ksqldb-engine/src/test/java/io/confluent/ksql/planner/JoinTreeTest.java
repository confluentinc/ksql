/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.planner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
import io.confluent.ksql.analyzer.Analysis.JoinInfo;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.JoinTree.Join;
import io.confluent.ksql.planner.JoinTree.Leaf;
import io.confluent.ksql.planner.JoinTree.Node;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JoinTreeTest {

  @Mock(name = "a") private AliasedDataSource a;
  @Mock(name = "b") private AliasedDataSource b;
  @Mock(name = "c") private AliasedDataSource c;
  @Mock(name = "d") private AliasedDataSource d;

  @Mock private JoinInfo j1;
  @Mock private JoinInfo j2;

  @Before
  public void setUp() {
    when(a.getAlias()).thenReturn(SourceName.of("a"));
    when(b.getAlias()).thenReturn(SourceName.of("b"));
    when(c.getAlias()).thenReturn(SourceName.of("c"));
  }

  @Test
  public void handlesBasicTwoWayJoin() {
    // Given:
    when(j1.getLeftSource()).thenReturn(a);
    when(j1.getRightSource()).thenReturn(b);
    final List<JoinInfo> joins = ImmutableList.of(j1);

    // When:
    final Node root = JoinTree.build(joins);

    // Then:
    assertThat(root, instanceOf(Join.class));
    assertThat(((Join) root).getLeft(), is(new JoinTree.Leaf(a)));
    assertThat(((Join) root).getRight(), is(new JoinTree.Leaf(b)));
    assertThat(((Join) root).getInfo(), is(j1));
  }

  @Test
  public void handlesLeftThreeWayJoin() {
    // Given:
    when(j1.getLeftSource()).thenReturn(a);
    when(j1.getRightSource()).thenReturn(b);
    when(j2.getLeftSource()).thenReturn(a);
    when(j2.getRightSource()).thenReturn(c);
    final List<JoinInfo> joins = ImmutableList.of(j1, j2);

    // When:
    final Node root = JoinTree.build(joins);

    // Then:
    assertThat(root, instanceOf(Join.class));
    assertThat(root, is(
        new Join(
            new Join(
                new Leaf(a), new Leaf(b), j1
            ),
            new Leaf(c),
            j2
        )
    ));
  }

  @Test
  public void handlesRightThreeWayJoin() {
    // Given:
    when(j1.getLeftSource()).thenReturn(a);
    when(j1.getRightSource()).thenReturn(b);
    when(j2.getLeftSource()).thenReturn(c);
    when(j2.getRightSource()).thenReturn(a);
    final List<JoinInfo> joins = ImmutableList.of(j1, j2);

    // When:
    final Node root = JoinTree.build(joins);

    // Then:
    assertThat(root, instanceOf(Join.class));
    assertThat(root, is(
        new Join(
            new Leaf(c),
            new Join(
                new Leaf(a), new Leaf(b), j1
            ),
            j2
        )
    ));
  }

  @Test
  public void outputsCorrectJoinTreeString() {
    // Given:
    when(j1.getLeftSource()).thenReturn(a);
    when(j1.getRightSource()).thenReturn(b);
    when(j2.getLeftSource()).thenReturn(a);
    when(j2.getRightSource()).thenReturn(c);
    final List<JoinInfo> joins = ImmutableList.of(j1, j2);

    // When:
    final Node root = JoinTree.build(joins);

    // Then:
    assertThat(root.debugString(0), is(
        "⋈\n"
            + "+--⋈\n"
            + "   +--a\n"
            + "   +--b\n"
            + "+--c"
    ));
  }

  @Test
  public void shouldThrowOnSelfJoin() {
    // Given:
    when(j1.getLeftSource()).thenReturn(a);
    when(j1.getRightSource()).thenReturn(a);
    final List<JoinInfo> joins = ImmutableList.of(j1);

    // When:
    final KsqlException e = assertThrows(KsqlException.class, () -> JoinTree.build(joins));

    // Then:
    assertThat(e.getMessage(), containsString("Cannot perform circular join"));
  }

  @Test
  public void shouldThrowOnCircularJoin() {
    // Given:
    when(j1.getLeftSource()).thenReturn(a);
    when(j1.getRightSource()).thenReturn(b);
    when(j2.getLeftSource()).thenReturn(a);
    when(j2.getRightSource()).thenReturn(b);
    final List<JoinInfo> joins = ImmutableList.of(j1, j2);

    // When:
    final KsqlException e = assertThrows(KsqlException.class, () -> JoinTree.build(joins));

    // Then:
    assertThat(e.getMessage(), containsString("Cannot perform circular join"));
  }

  @Test
  public void shouldThrowOnMissingSource() {
    // Given:
    when(j1.getLeftSource()).thenReturn(a);
    when(j1.getRightSource()).thenReturn(b);
    when(j2.getLeftSource()).thenReturn(c);
    when(j2.getRightSource()).thenReturn(d);
    final List<JoinInfo> joins = ImmutableList.of(j1, j2);

    // When:
    final KsqlException e = assertThrows(KsqlException.class, () -> JoinTree.build(joins));

    // Then:
    assertThat(e.getMessage(), containsString("neither source in the join is the FROM source"));
  }

}