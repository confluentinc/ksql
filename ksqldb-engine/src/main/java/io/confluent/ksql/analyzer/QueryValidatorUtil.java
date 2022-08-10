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

package io.confluent.ksql.analyzer;

import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.util.KsqlException;
import java.util.Collection;
import java.util.stream.Collectors;

public final class QueryValidatorUtil {

  private QueryValidatorUtil() {
  }

  /**
   * In the event that a user created a stream/table with a user column with the same
   * name as a newly added pseudocolumn (i.e., user had an existing stream/table and
   * then upgraded to a newer version of ksqlDB which introduced a new pseudocolumn
   * with a conflicting name), then we disallow any new queries (persistent, push, and
   * pull) against this existing stream/table as the ksqlDB engine does not properly
   * handle the name conflict.
   */
  static void validateNoUserColumnsWithSameNameAsPseudoColumns(final Analysis analysis) {

    final String disallowedNames = analysis.getAllDataSources()
        .stream()
        .map(AliasedDataSource::getDataSource)
        .map(DataSource::getSchema)
        .map(LogicalSchema::value)
        .flatMap(Collection::stream)
        .map(Column::name)
        .filter(name -> SystemColumns.isPseudoColumn(name))
        .map(ColumnName::toString)
        .collect(Collectors.joining(", "));

    if (disallowedNames.length() > 0) {
      throw new KsqlException(
          "Your stream/table has columns with the same name as newly introduced pseudocolumns in"
              + " ksqlDB, and cannot be queried as a result. The conflicting names are: "
              + disallowedNames + ".\n"
      );
    }
  }
}
