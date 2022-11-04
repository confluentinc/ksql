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

import io.confluent.ksql.util.KsqlStatementException;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqlLang {
  private static final Logger LOG = LoggerFactory.getLogger(KsqlLang.class);

  private final KsqlLogicalPlanner planner;

  public static class KsqlTable extends AbstractTable implements ScannableTable, StreamableTable {

    @Override
    public Table stream() {
      return null;
    }

    @Override
    public Enumerable<Object[]> scan(final DataContext root) {
      return null;
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
      final RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
      final RelDataType t1 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
      final RelDataType t2 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.CHAR), true);
      final RelDataType t3 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.CHAR), true);
      builder.add("ID", t1);
      builder.add("NAME", t2);
      builder.add("OWNERID", t3);
      return builder.build();
    }
  }

  public KsqlLang() {
    final KsqlCatalog catalog = new KsqlCatalog();
    catalog.addSampleTables();
    planner = new KsqlLogicalPlanner(catalog);
  }

  public KsqlLogicalPlanner.KsqlLogicalPlan getLogicalPlan(final String statement) {
    return planner.getLogicalPlan(statement);
  }
}
