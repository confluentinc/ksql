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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
import io.confluent.ksql.analyzer.Analysis.JoinInfo;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.planner.plan.JoinNode.JoinType;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;

/**
 * {@code JoinTree} constructs the logical order for which the
 * joins should be executed. At the moment, there is no optimization
 * done and it simply follows the order of execution that the user
 * indicates.
 *
 * <p>The algorithm is simple: the root is the very first source that
 * we encounter (in the case of a single join, we ensure that the left
 * root is the FROM source). From then on, any join that happens will
 * happen on the right, creating a left-deep tree.
 *
 * <p>For example, take the following join statement:
 * <pre>
 * {@code
 *    SELECT * FROM a
 *      JOIN b ON a.id = b.id
 *      JOIN c ON a.id = c.id;
 * }
 * </pre>
 * The resulting join tree would look like:
 * <pre>
 * {@code
 *       ⋈
 *      / \
 *     ⋈   C
 *    / \
 *   A   B
 * }
 * </pre>
 * </p>
 */
final class JoinTree {

  private JoinTree() {
  }

  /**
   * Constructs the join tree given a list of {@code JoinInfo}
   *
   * @param joins the joins
   * @return the tree indicating the order of the join
   * @see JoinTree
   */
  public static Node build(final List<JoinInfo> joins) {
    Node root = null;

    for (final JoinInfo join : joins) {
      if (root == null) {
        root = new Leaf(join.getLeftSource());
      }

      if (root.containsSource(join.getRightSource()) && root.containsSource(join.getLeftSource())) {
        throw new KsqlException("Cannot perform circular join - both " + join.getRightSource()
            + " and " + join.getLeftJoinExpression()
            + " are already included in the current join tree: " + root.debugString(0));
      } else if (root.containsSource(join.getLeftSource())) {
        root = new Join(root, new Leaf(join.getRightSource()), join);
      } else if (root.containsSource(join.getRightSource())) {
        root = new Join(root, new Leaf(join.getLeftSource()), join.flip());
      } else {
        throw new KsqlException(
            "Cannot build JOIN tree; neither source in the join is the FROM source or included "
                + "in a previous JOIN: " + join + ". The current join tree is "
                + root.debugString(0)
        );
      }
    }

    return root;
  }

  /**
   * A node in the {@code JoinTree} that represents either a Leaf node or a Join
   * node.
   */
  interface Node {

    /**
     * @param dataSource the data source to search for
     * @return whether or not this node already references the {@code dataSource}
     */
    boolean containsSource(AliasedDataSource dataSource);

    /**
     * @return a debug string that pretty prints the tree
     */
    String debugString(int indent);

    /**
     * An {@code Expression} that is part of this Node's equivalence set evaluates
     * to the same value as the key for this join. Consider the following JOIN:
     * <pre>{@code
     *  SELECT * FROM A JOIN B on A.id = B.id + 1
     *                  JOIN C on A.id = C.id - 1;
     * }</pre>
     * The equivalence set for the above join would be {@code {A.id, B.id + 1, c.id - 1}}
     * since all of those expressions evaluate to the same value.
     */
    Set<Expression> joinEquivalenceSet();

    /**
     * The set of columns that can be used in the projection to include the key.
     *
     * <p>Consider the following JOIN:
     * <pre>{@code
     *  SELECT * FROM A JOIN B on A.id = B.id + 1
     *                  JOIN C on A.id = C.id;
     * }</pre>
     * The viable keys would be {@code {A.id, c.id}}.
     *
     * @return the viable key columns.
     */
    List<QualifiedColumnReferenceExp> viableKeyColumns();
  }

  static class Join implements Node {

    private final Node left;
    private final Node right;
    private final JoinInfo info;

    Join(final Node left, final Node right, final JoinInfo info) {
      this.left = left;
      this.right = right;
      this.info = info;
    }

    public JoinInfo getInfo() {
      return info;
    }

    public Node getLeft() {
      return left;
    }

    public Node getRight() {
      return right;
    }

    @Override
    public boolean containsSource(final AliasedDataSource dataSource) {
      return left.containsSource(dataSource) || right.containsSource(dataSource);
    }

    @Override
    public String debugString(final int indent) {
      return "⋈\n"
          + StringUtils.repeat(' ', indent) + "+--"
          + left.debugString(indent + 3) + "\n"
          + StringUtils.repeat(' ', indent) + "+--"
          + right.debugString(indent + 3);
    }

    @Override
    public Set<Expression> joinEquivalenceSet() {
      // the algorithm to compute the keys on a tree recursively
      // checks to see if the keys from subtrees are equivalent to
      // the existing join criteria. take the following tree and
      // join conditions as example:
      //
      //            ⋈
      //          /   \
      //         ⋈     ⋈
      //        /  \  / \
      //       A   B C   D
      //
      // A JOIN B on A.id = B.id as AB
      // C JOIN D on C.id = D.id as BC
      // AB JOIN CD on A.id = C.id + 1 as ABCD
      //
      // The final topic would be partitioned on A.id, which is equivalent
      // to any one of [A.id, B.id, C.id+1].
      //
      // We can compute this set by checking if either the left or right side
      // of the join expression is contained within the equivalence key set
      // of the child node. If it is contained, then we know that the child
      // equivalence set can be included in the parent equivalence set
      //
      // In the example above, the left side of the join, A.id, is in the AB
      // child set. This means we can add all of AB's keys to the output.
      // `C.id + 1` on the other hand, is not in either AB or BC's child set
      // so we do not include them in the output set.
      //
      // We always include both sides of the current join in the output set,
      // since we know the key will be the equivalence of those

      if (info.getType() == JoinType.OUTER) {
        // The key column of OUTER joins are not equivalent to either join
        // expression, (as either can be null), and hence return an empty equivalence
        // set.
        return ImmutableSet.of();
      }

      final Set<Expression> keys = ImmutableSet.of(
          info.getLeftJoinExpression(),
          info.getRightJoinExpression()
      );

      final Set<Expression> lefts = left.joinEquivalenceSet();
      final Set<Expression> rights = right.joinEquivalenceSet();

      final boolean includeLeft = !Sets.intersection(lefts, keys).isEmpty();
      final boolean includeRight = !Sets.intersection(rights, keys).isEmpty();

      if (includeLeft && includeRight) {
        return Sets.union(keys, Sets.union(lefts, rights));
      } else if (includeLeft) {
        return Sets.union(keys, lefts);
      } else if (includeRight) {
        return Sets.union(keys, rights);
      } else {
        return keys;
      }
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public List<QualifiedColumnReferenceExp> viableKeyColumns() {

      if (info.getType() == JoinType.OUTER) {
        // Full outer joins have no source column that is equivalent to the key column of the result
        return ImmutableList.of();
      }

      final Set<Expression> equiv = joinEquivalenceSet();

      return Streams
          .concat(
              left.viableKeyColumns().stream(),
              Stream.of(info.getLeftJoinExpression()),
              right.viableKeyColumns().stream(),
              Stream.of(info.getRightJoinExpression())
          )
          .filter(e -> e instanceof QualifiedColumnReferenceExp)
          .map(QualifiedColumnReferenceExp.class::cast)
          .distinct()
          .filter(equiv::contains)
          .collect(Collectors.toList());
    }

    @Override
    public String toString() {
      return "Join{" + "left=" + left
          + ", right=" + right
          + ", info=" + info
          + '}';
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final Join that = (Join) o;
      return Objects.equals(left, that.left)
          && Objects.equals(right, that.right)
          && Objects.equals(info, that.info);
    }

    @Override
    public int hashCode() {
      return Objects.hash(left, right, info);
    }
  }

  static class Leaf implements Node {
    private final AliasedDataSource source;

    Leaf(final AliasedDataSource source) {
      this.source = source;
    }

    public AliasedDataSource getSource() {
      return source;
    }

    @Override
    public boolean containsSource(final AliasedDataSource dataSource) {
      return source.equals(dataSource);
    }

    @Override
    public String debugString(final int indent) {
      return source.getAlias().toString(FormatOptions.noEscape());
    }

    @Override
    public Set<Expression> joinEquivalenceSet() {
      return ImmutableSet.of(); // Leaf nodes don't have join equivalence sets
    }

    @Override
    public List<QualifiedColumnReferenceExp> viableKeyColumns() {
      return ImmutableList.of();
    }

    @Override
    public String toString() {
      return "Leaf{" + "source=" + source + '}';
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Leaf that = (Leaf) o;
      return Objects.equals(source, that.source);
    }

    @Override
    public int hashCode() {
      return Objects.hash(source);
    }
  }

}
