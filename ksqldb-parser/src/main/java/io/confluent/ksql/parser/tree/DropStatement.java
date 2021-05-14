/*
 * Copyright 2021 Confluent Inc.
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
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

@Immutable
public abstract class DropStatement extends Statement {

  private final SourceName name;
  private final boolean ifExists;
  private final boolean deleteTopic;

  DropStatement(
      final Optional<NodeLocation> location,
      final SourceName name,
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

  public SourceName getName() {
    return name;
  }

  public boolean isDeleteTopic() {
    return deleteTopic;
  }

  public abstract DropStatement withoutDeleteClause();

  @Override
  public int hashCode() {
    return Objects.hash(name, ifExists, deleteTopic);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof DropStatement)) {
      return false;
    }
    final DropStatement o = (DropStatement) obj;
    return Objects.equals(name, o.name)
        && (ifExists == o.ifExists)
        && (deleteTopic == o.deleteTopic);
  }
}
