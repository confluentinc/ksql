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

import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.parser.NodeLocation;

public class InvalidColumnException extends KsqlException {

  public InvalidColumnException(
      final ColumnReferenceExp column,
      final String message
  ) {
    this("", column, message);
  }

  public InvalidColumnException(
      final String prefix,
      final ColumnReferenceExp column,
      final String message
  ) {
    super(buildMessage(prefix, column, message));
  }

  private static String buildMessage(
      final String prefix,
      final ColumnReferenceExp column,
      final String message
  ) {
    return NodeLocation.asPrefix(column.getLocation())
        + prefix
        + (prefix.isEmpty() ? "C" : " c")
        + "olumn '" + column + "' " + message;
  }
}
