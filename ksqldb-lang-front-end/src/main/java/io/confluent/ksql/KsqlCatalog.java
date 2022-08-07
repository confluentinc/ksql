/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;

public class KsqlCatalog {
  private static class KsqlSchema extends AbstractSchema {
    private String name;

    public KsqlSchema(final String name) {
      this.name = name;
    }

    private final Map<String, Table> tableMap = new HashMap<>();

    @Override
    protected Map<String, Table> getTableMap() {
      return tableMap;
    }

    public void addTable(final String name, final KsqlTable table) {
      tableMap.put(name, table);
    }
  }

  private final SchemaPlus schema;
  private final Map<String, KsqlSchema> schemas = new HashMap<>();

  public KsqlCatalog() {
    this.schema = CalciteSchema
        .createRootSchema(false)
        .plus();

  }

  public void addTable(final String schemaName, final String tableName, final KsqlTable table) {
    final boolean shouldAdd = !schemas.containsKey(schemaName);
    final KsqlSchema ksqlSchema = schemas.computeIfAbsent(schemaName, KsqlSchema::new);
    if (shouldAdd) {
      schema.add(schemaName, ksqlSchema);
    }
    ksqlSchema.addTable(tableName, table);
  }

  public SchemaPlus getSchema() {
    return schema;
  }

  public void addSampleTables() {
    final Random random = new Random(42);

    this.addTable(
        "sample",
        "ORDERS",
        new KsqlTable(
            tf -> tf
                .builder()
                .add("ROWTIME", SqlTypeName.TIMESTAMP)
                .add("ID", SqlTypeName.INTEGER)
                .add("PRODUCT", SqlTypeName.VARCHAR, 10)
                .add("UNITS", SqlTypeName.INTEGER)
                .build(),
            ImmutableList.of(
                new Object[]{now(random), 4, "asdf", 5},
                new Object[]{now(random), 5, "qwre", 7},
                new Object[]{now(random), 3, "zxcvzxcv", 99}
            )
        )
    );

    this.addTable(
        "sample",
        "PRODUCTS",
        new KsqlTable(
            tf -> tf
                .builder()
                .add("ROWTIME", SqlTypeName.TIMESTAMP)
                .add("ID", SqlTypeName.VARCHAR, 10)
                .add("PRICE", SqlTypeName.DOUBLE)
                .build(),
            ImmutableList.of(
                new Object[]{now(random), "asdf", 5.55},
                new Object[]{now(random), "qwre", 7.32},
                new Object[]{now(random), "zxcvzxcv", 99.90}
            )
        )
    );
  }
  private static long now(final Random random) {
    return System.currentTimeMillis() + random.nextInt(60_000);
  }
}

