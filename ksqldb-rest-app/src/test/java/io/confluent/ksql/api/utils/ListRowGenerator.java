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

package io.confluent.ksql.api.utils;

import io.confluent.ksql.GenericRow;
import java.util.Iterator;
import java.util.List;

public class ListRowGenerator implements RowGenerator {

  private final List<String> columnNames;
  private final List<String> columnTypes;
  private final Iterator<GenericRow> iter;

  public ListRowGenerator(final List<String> columnNames, final List<String> columnTypes,
      final List<GenericRow> rows) {
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.iter = rows.iterator();
  }

  @Override
  public List<String> getColumnNames() {
    return columnNames;
  }

  @Override
  public List<String> getColumnTypes() {
    return columnTypes;
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

