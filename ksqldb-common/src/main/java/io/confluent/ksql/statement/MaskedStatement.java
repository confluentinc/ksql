/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.statement;

import java.util.Objects;

public final class MaskedStatement {

  public static final MaskedStatement EMPTY_MASKED_STATEMENT = MaskedStatement.of("");
  private String statementText;

  private MaskedStatement(final String statementText) {
    this.statementText = statementText;
  }

  public static MaskedStatement of(final String statement) {
    return new MaskedStatement(statement);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final MaskedStatement that = (MaskedStatement) o;
    return Objects.equals(statementText, that.statementText);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statementText);
  }

  public boolean isEmpty() {
    return statementText == null || statementText.isEmpty();
  }

  @Override
  public String toString() {
    return statementText;
  }
}
