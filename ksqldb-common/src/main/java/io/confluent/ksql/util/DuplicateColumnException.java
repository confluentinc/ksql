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

import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.Column.Namespace;

public class DuplicateColumnException extends KsqlException {
  private final Namespace namespace;
  private final Column column;

  public DuplicateColumnException(final Namespace namespace, final Column column) {
    super(buildMessage(namespace, column));
    this.namespace = namespace;
    this.column = column;
  }

  public Namespace getNamespace() {
    return namespace;
  }

  public Column getColumn() {
    return column;
  }

  private static String buildMessage(final Namespace namespace, final Column column) {
    return String.format(
        "Duplicate %s columns found in schema: %s",
        namespace.name().toLowerCase(),
        column
    );
  }
}
