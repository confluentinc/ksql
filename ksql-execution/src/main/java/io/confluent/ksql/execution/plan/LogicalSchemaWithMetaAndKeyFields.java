/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.plan;

import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Objects;

public final class LogicalSchemaWithMetaAndKeyFields {
  private final LogicalSchema schema;

  LogicalSchemaWithMetaAndKeyFields(final LogicalSchema schema) {
    this.schema = schema;
  }

  public static LogicalSchemaWithMetaAndKeyFields fromOriginal(
      final String alias,
      final LogicalSchema schema) {
    return new LogicalSchemaWithMetaAndKeyFields(
        schema.withAlias(alias).withMetaAndKeyFieldsInValue());
  }

  public static LogicalSchemaWithMetaAndKeyFields fromTransformed(final LogicalSchema schema) {
    return new LogicalSchemaWithMetaAndKeyFields(schema);
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public LogicalSchema getOriginalSchema() {
    return schema.withoutMetaAndKeyFieldsInValue().withoutAlias();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final LogicalSchemaWithMetaAndKeyFields that = (LogicalSchemaWithMetaAndKeyFields) o;
    return Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema);
  }
}
