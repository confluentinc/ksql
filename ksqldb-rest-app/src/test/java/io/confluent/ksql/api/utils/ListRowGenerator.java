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

package io.confluent.ksql.api.utils;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Iterator;
import java.util.List;

public class ListRowGenerator implements RowGenerator {

  private final ImmutableList<String> columnNames;
  private final ImmutableList<String> columnTypes;
  private final LogicalSchema logicalSchema;
  private final Iterator<GenericRow> iter;

  public ListRowGenerator(final List<String> columnNames, final List<String> columnTypes,
      final LogicalSchema logicalSchema, final List<GenericRow> rows) {
    this.columnNames = ImmutableList.copyOf(columnNames);
    this.columnTypes = ImmutableList.copyOf(columnTypes);
    this.logicalSchema = logicalSchema;
    this.iter = rows.iterator();
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "columnNames is ImmutableList")
  public List<String> getColumnNames() {
    return columnNames;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "columnTypes is ImmutableList")
  public List<String> getColumnTypes() {
    return columnTypes;
  }

  @Override
  public LogicalSchema getLogicalSchema() {
    return logicalSchema;
  }

  @Override
  public GenericRow getNext() {
    if (iter.hasNext()) {
      return iter.next();
    } else {
      return null;
    }
  }

}

