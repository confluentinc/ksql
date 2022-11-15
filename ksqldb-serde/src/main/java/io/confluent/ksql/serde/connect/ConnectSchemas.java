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

package io.confluent.ksql.serde.connect;

import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SchemaConverters.SqlToConnectTypeConverter;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import java.util.List;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public final class ConnectSchemas {

  private ConnectSchemas() {
  }

  /**
   * Convert a list of columns into a Connect Struct schema with fields to match.
   *
   * @param columns the list of columns.
   * @return the Struct schema.
   */
  public static ConnectSchema columnsToConnectSchema(final List<? extends SimpleColumn> columns) {
    final SqlToConnectTypeConverter converter = SchemaConverters.sqlToConnectConverter();

    final SchemaBuilder builder = SchemaBuilder.struct();
    for (final SimpleColumn column : columns) {
      final Schema colSchema = converter.toConnectSchema(column.type());
      builder.field(column.name().text(), colSchema);
    }

    return (ConnectSchema) builder.build();
  }
}
