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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class DescribeConnector extends Statement {

  private final String connectorName;

  public DescribeConnector(
      final Optional<NodeLocation> location,
      final String connectorName
  ) {
    super(location);
    this.connectorName = Objects.requireNonNull(connectorName, "connectorName");
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
    final DescribeConnector that = (DescribeConnector) o;
    return Objects.equals(connectorName, that.connectorName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectorName);
  }

  @Override
  public String toString() {
    return "DescribeConnector{"
        + "connectorName='" + connectorName + '\''
        + '}';
  }
}
