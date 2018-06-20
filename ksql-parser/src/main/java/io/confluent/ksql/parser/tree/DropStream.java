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

public class DropStream
    extends AbstractStreamDropStatement implements DdlStatement {

  private final QualifiedName streamName;
  private final boolean ifExists;
  private final boolean deleteTopic;

  public DropStream(QualifiedName tableName, boolean ifExists, boolean deleteTopic) {
    this(Optional.empty(), tableName, ifExists, deleteTopic);
  }

  public DropStream(NodeLocation location,
                    QualifiedName tableName,
                    boolean ifExists,
                    boolean deleteTopic) {
    this(Optional.of(location), tableName, ifExists, deleteTopic);
  }

  private DropStream(Optional<NodeLocation> location,
                     QualifiedName streamName,
                     boolean ifExists,
                     boolean deleteTopic) {
    super(location);
    this.streamName = streamName;
    this.ifExists = ifExists;
    this.deleteTopic = deleteTopic;
  }

  public QualifiedName getName() {
    return streamName;
  }

  public boolean getIfExists() {
    return ifExists;
  }

  public QualifiedName getStreamName() {
    return streamName;
  }

  public boolean isDeleteTopic() {
    return deleteTopic;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDropStream(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamName, ifExists);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    DropStream o = (DropStream) obj;
    return Objects.equals(streamName, o.streamName)
           && (ifExists == o.ifExists)
           && (deleteTopic == o.deleteTopic);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("tableName", streamName)
        .add("ifExists", ifExists)
        .add("deleteTopic", deleteTopic)
        .toString();
  }
}
