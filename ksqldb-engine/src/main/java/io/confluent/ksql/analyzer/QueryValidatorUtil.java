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
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Collection;

public class QueryValidatorUtil {

  private QueryValidatorUtil() {

  }

  static void validateNoUserColumnsWithSameNameAsNewPseudoColumns(final Analysis analysis) {

    final KsqlConfig ksqlConfig = analysis.getKsqlConfig();

    boolean hasDisallowedPseudoColumns = analysis.getAllDataSources()
        .stream()
        .map(AliasedDataSource::getDataSource)
        .map(DataSource::getSchema)
        .map(LogicalSchema::value)
        .flatMap(Collection::stream)
        .map(Column::name)
        .anyMatch(name -> SystemColumns.isPseudoColumn(name, ksqlConfig));

    if (hasDisallowedPseudoColumns) {
      throw new KsqlException("You cannot query a stream or table that "
          + "has user columns with the same name as new pseudocolumns.\n"
          + "To allow for new queries on this source, downgrade your ksql "
          + "version to a release before the conflicting pseudocolumns "
          + "were introduced."
      );
    }
  }

}
