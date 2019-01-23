/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser.tree;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import java.util.Optional;

public abstract class AbstractStreamDropStatement extends Statement {

  private final QualifiedName name;
  private final boolean ifExists;
  private final boolean deleteTopic;

  public AbstractStreamDropStatement(final Optional<NodeLocation> location,
                                     final QualifiedName name,
                                     final boolean deleteTopic,
                                     final boolean ifExists) {
    super(location);
    this.name = name;
    this.deleteTopic = deleteTopic;
    this.ifExists = ifExists;
  }

  public QualifiedName getName() {
    return name;
  }

  public boolean isDeleteTopic() {
    return deleteTopic;
  }

  public boolean getIfExists() {
    return ifExists;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final AbstractStreamDropStatement that = (AbstractStreamDropStatement) o;
    return deleteTopic == that.deleteTopic
        && ifExists == that.ifExists
        && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, deleteTopic, ifExists);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("deleteTopic", deleteTopic)
        .add("ifExists", ifExists)
        .toString();
  }

}
