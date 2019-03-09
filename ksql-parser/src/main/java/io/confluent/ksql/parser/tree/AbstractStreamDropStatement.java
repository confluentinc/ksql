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
import java.util.Objects;
import java.util.Optional;

@Immutable
public abstract class AbstractStreamDropStatement extends Statement {

  private final QualifiedName name;
  private final boolean ifExists;
  private final boolean deleteTopic;

  AbstractStreamDropStatement(
      final Optional<NodeLocation> location,
      final QualifiedName name,
      final boolean ifExists,
      final boolean deleteTopic
  ) {
    super(location);
    this.name = Objects.requireNonNull(name, "name");
    this.ifExists = ifExists;
    this.deleteTopic = deleteTopic;
  }

  public boolean getIfExists() {
    return ifExists;
  }

  public QualifiedName getName() {
    return name;
  }

  public boolean isDeleteTopic() {
    return deleteTopic;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, ifExists, deleteTopic);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof AbstractStreamDropStatement)) {
      return false;
    }
    final AbstractStreamDropStatement o = (AbstractStreamDropStatement) obj;
    return Objects.equals(name, o.name)
        && (ifExists == o.ifExists)
        && (deleteTopic == o.deleteTopic);
  }
}
