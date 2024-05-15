/*
 * Copyright 2022 Confluent Inc.
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

import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

public abstract class AssertResource extends Statement {
  protected final Optional<WindowTimeClause> timeout;
  protected final boolean exists;

  protected AssertResource(
      final Optional<NodeLocation> location,
      final Optional<WindowTimeClause> timeout,
      final boolean exists
  ) {
    super(location);
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.exists = exists;
  }

  public Optional<WindowTimeClause> getTimeout() {
    return timeout;
  }

  public boolean checkExists() {
    return exists;
  }
}
