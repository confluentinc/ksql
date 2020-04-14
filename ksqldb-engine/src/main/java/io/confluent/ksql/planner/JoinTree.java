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

import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
import io.confluent.ksql.analyzer.Analysis.JoinInfo;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
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
