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

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class KsqlTable implements ScannableTable {

  private final RelProtoDataType protoDataType;
  private final List<Object[]> data;
  private final Supplier<List<Object[]>> dataFn;

  public KsqlTable(final RelProtoDataType protoDataType, final List<Object[]> data) {
    this.protoDataType = protoDataType;
    this.data = data;
    this.dataFn = null;
  }

  public KsqlTable(final RelProtoDataType protoDataType, final Supplier<List<Object[]>> data) {
    this.protoDataType = protoDataType;
    this.data = null;
    this.dataFn = data;
  }

  public Enumerable<Object[]> scan(final DataContext root) {
    if (data != null) {
      return Linq4j.asEnumerable(data);
    } else {
      final Supplier<List<Object[]>> listSupplier = Objects.requireNonNull(dataFn);
      return Linq4j.asEnumerable(listSupplier.get());
    }
  }

  @Override
  public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
    return protoDataType.apply(typeFactory);
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.TABLE;
  }

  @Override
  public boolean isRolledUp(final String column) {
    return false;
  }

  @Override
  public boolean rolledUpColumnValidInsideAgg(final String column, final SqlCall call,
                                              @Nullable final SqlNode parent, @Nullable final CalciteConnectionConfig config) {
    return false;
  }

}

