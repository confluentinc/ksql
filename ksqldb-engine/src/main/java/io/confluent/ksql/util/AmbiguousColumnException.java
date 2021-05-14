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

package io.confluent.ksql.util;

import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.name.SourceName;
import java.util.Collection;
import java.util.Objects;

public class AmbiguousColumnException extends InvalidColumnException {

  public AmbiguousColumnException(
      final UnqualifiedColumnReferenceExp column,
      final Collection<SourceName> possibleSources
  ) {
    super(
        column,
        "is ambiguous. Could be "
            + GrammaticalJoiner.or().join(
            possibleSources.stream()
                .map(source -> new QualifiedColumnReferenceExp(source, column.getColumnName()))
                .map(Objects::toString)
                .sorted()
        ) + "."
    );
  }
}
