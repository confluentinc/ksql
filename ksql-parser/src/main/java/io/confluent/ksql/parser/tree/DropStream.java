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

import java.util.Optional;

public class DropStream
    extends AbstractStreamDropStatement implements ExecutableDdlStatement {

  public DropStream(
      final QualifiedName tableName,
      final boolean ifExists,
      final boolean deleteTopic) {
    this(Optional.empty(), tableName, ifExists, deleteTopic);
  }

  public DropStream(final NodeLocation location,
                    final QualifiedName tableName,
                    final boolean ifExists,
                    final boolean deleteTopic) {
    this(Optional.of(location), tableName, ifExists, deleteTopic);
  }

  public DropStream(final Optional<NodeLocation> location,
                     final QualifiedName streamName,
                     final boolean ifExists,
                     final boolean deleteTopic) {
    super(location, streamName, deleteTopic, ifExists);
  }

  public QualifiedName getStreamName() {
    return getName();
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitDropStream(this, context);
  }

}
