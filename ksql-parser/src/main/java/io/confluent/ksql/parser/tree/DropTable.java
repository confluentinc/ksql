/**
 * Copyright 2017 Confluent Inc.
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
 **/

package io.confluent.ksql.parser.tree;

import static com.google.common.base.MoreObjects.toStringHelper;

import java.util.Objects;
import java.util.Optional;

public class DropTable extends AbstractStreamDropStatement implements DdlStatement {

  private final QualifiedName tableName;
  private final boolean ifExists;
  private final boolean deleteTopic;

  public DropTable(
      final QualifiedName tableName,
      final boolean ifExists,
      final boolean deleteTopic) {
    this(Optional.empty(), tableName, ifExists, deleteTopic);
  }

  public DropTable(final Optional<NodeLocation> location,
                    final QualifiedName tableName,
                    final boolean ifExists,
                    final boolean deleteTopic) {
    super(location);
    this.tableName = tableName;
    this.ifExists = ifExists;
    this.deleteTopic = deleteTopic;
  }

  public QualifiedName getName() {
    return tableName;
  }

  public boolean getIfExists() {
    return ifExists;
  }

  public QualifiedName getTableName() {
    return tableName;
  }

  public boolean isDeleteTopic() {
    return deleteTopic;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitDropTable(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, ifExists);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final DropTable o = (DropTable) obj;
    return Objects.equals(tableName, o.tableName)
           && (ifExists == o.ifExists)
           && (deleteTopic == o.deleteTopic);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("tableName", tableName)
        .add("ifExists", ifExists)
        .add("deleteTopic", deleteTopic)
        .toString();
  }
}
