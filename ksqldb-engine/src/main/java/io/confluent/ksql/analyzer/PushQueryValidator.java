/*
 * Copyright 2019 Confluent Inc.
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
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.util.KsqlException;

public class PushQueryValidator implements QueryValidator {

  @Override
  public void validate(final Analysis analysis) {
    failPersistentQueryOnWindowedTable(analysis);
    QueryValidatorUtil.validateNoUserColumnsWithSameNameAsPseudoColumns(analysis);
  }

  private static void failPersistentQueryOnWindowedTable(final Analysis analysis) {
    if (!analysis.getInto().isPresent()) {
      return;
    }
    if (analysis.getAllDataSources().stream().anyMatch(PushQueryValidator::isWindowedTable)) {
      throw new KsqlException("KSQL does not support persistent queries on windowed tables.");
    }
  }

  private static boolean isWindowedTable(final AliasedDataSource dataSource) {
    return dataSource.getDataSource().getDataSourceType() == DataSourceType.KTABLE
        && dataSource.getDataSource().getKsqlTopic().getKeyFormat().isWindowed();
  }
}
