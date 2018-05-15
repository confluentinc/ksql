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

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class DropTable extends AbstractStreamDropStatement implements DdlStatement {

  private final QualifiedName tableName;
  private final boolean exists;
  private final boolean deleteTopic;

  public DropTable(QualifiedName tableName, boolean exists, boolean deleteTopic) {
    this(Optional.empty(), tableName, exists, deleteTopic);
  }

  public DropTable(NodeLocation location,
                   QualifiedName tableName,
                   boolean exists,
                   boolean deleteTopic) {
    this(Optional.of(location), tableName, exists, deleteTopic);
  }

  private DropTable(Optional<NodeLocation> location,
                    QualifiedName tableName,
                    boolean exists,
                    boolean deleteTopic) {
    super(location);
    this.tableName = tableName;
    this.exists = exists;
    this.deleteTopic = deleteTopic;
  }

  public QualifiedName getName() {
    return tableName;
  }

  public boolean isExists() {
    return exists;
  }

  public QualifiedName getTableName() {
    return tableName;
  }

  public boolean isDeleteTopic() {
    return deleteTopic;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDropTable(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, exists);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    DropTable o = (DropTable) obj;
    return Objects.equals(tableName, o.tableName)
           && (exists == o.exists)
           && (deleteTopic == o.deleteTopic);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("tableName", tableName)
        .add("exists", exists)
        .add("deleteTopic", deleteTopic)
        .toString();
  }
}
