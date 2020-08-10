/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.parser;

import io.confluent.ksql.parser.tree.AssertStatement;
import io.confluent.ksql.parser.tree.CreateTable;
import java.util.Objects;
import java.util.Optional;

public class AssertTable extends AssertStatement<CreateTable> {

  public AssertTable(
      final Optional<NodeLocation> location,
      final CreateTable statement
  ) {
    super(location, statement);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getStatement());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AssertTable)) {
      return false;
    }
    final AssertTable that = (AssertTable) o;
    return Objects.equals(getStatement(), that.getStatement());
  }

  @Override
  public String toString() {
    return "AssertTable{"
        + "statement=" + getStatement()
        + '}';
  }

}
