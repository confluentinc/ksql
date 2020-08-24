/*
 * Copyright 2019 Confluent Inc.
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

import static com.google.common.base.MoreObjects.toStringHelper;

import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

public class DropConnector extends Statement {

  private final boolean ifExists;
  private final String connectorName;

  public DropConnector(
      final Optional<NodeLocation> location,
      final boolean ifExists,
      final String connectorName
  ) {
    super(location);
    this.ifExists = ifExists;
    this.connectorName = connectorName;
  }

  public boolean getIfExists() {
    return ifExists;
  }

  public String getConnectorName() {
    return connectorName;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DropConnector that = (DropConnector) o;
    return Objects.equals(connectorName, that.connectorName) && ifExists == that.ifExists;
  }

  @Override
  public int hashCode() {
    return Objects.hash(ifExists, connectorName);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
            .add("ifExists", ifExists)
            .add("connectorName", connectorName)
            .toString();
  }
}
