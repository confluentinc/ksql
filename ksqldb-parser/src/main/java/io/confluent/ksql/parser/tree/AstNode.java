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

package io.confluent.ksql.parser.tree;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.Node;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Optional;

@Immutable
public abstract class AstNode extends Node {
  private final Optional<NodeLocation> endLocation;

  protected AstNode(final Optional<NodeLocation> location,
                    final Optional<NodeLocation> endLocation) {
    super(location);
    this.endLocation = endLocation;
  }

  /**
   * Accessible for {@link AstVisitor}, use {@link AstVisitor#process(AstNode, Object)} instead.
   */
  protected abstract <R, C> R accept(AstVisitor<R, C> visitor, C context);

  public Optional<NodeLocation> getEndLocation() {
    return this.endLocation;
  }
}
