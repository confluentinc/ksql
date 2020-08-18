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

package io.confluent.ksql.parser.tree;

import io.confluent.ksql.parser.NodeLocation;
import java.util.Optional;

/**
 * Indicates that the given {@link io.confluent.ksql.parser.Node} is used
 * for testing purposes.
 */
public abstract class AssertStatement extends AstNode  {

  protected AssertStatement(
      final Optional<NodeLocation> location
  ) {
    super(location);
  }

}
