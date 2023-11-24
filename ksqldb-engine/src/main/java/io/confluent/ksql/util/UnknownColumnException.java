/*
 * Copyright 2020 Confluent Inc.
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

import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;

public class UnknownColumnException extends InvalidColumnException  {

  public UnknownColumnException(final ColumnReferenceExp column) {
    this("", column);
  }

  public UnknownColumnException(final String prefix, final ColumnReferenceExp column) {
    this(prefix, column, "cannot be resolved.");
  }

  public UnknownColumnException(
      final String prefix,
      final ColumnReferenceExp column,
      final String message
  ) {
    super(prefix, column, message);
  }
}
