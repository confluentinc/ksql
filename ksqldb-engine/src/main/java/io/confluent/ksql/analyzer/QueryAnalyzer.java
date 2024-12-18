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

package io.confluent.ksql.analyzer;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;

public class QueryAnalyzer {

  private final Analyzer analyzer;
  private final QueryValidator pullQueryValidator;
  private final QueryValidator pushQueryValidator;

  public QueryAnalyzer(
      final MetaStore metaStore,
      final String outputTopicPrefix,
      final boolean pullLimitClauseEnabled
  ) {
    this(
        new Analyzer(
                metaStore,
                outputTopicPrefix,
                pullLimitClauseEnabled),

        new PullQueryValidator(),
        new PushQueryValidator()
    );
  }

  @VisibleForTesting
  QueryAnalyzer(
      final Analyzer analyzer,
      final QueryValidator pullQueryValidator,
      final QueryValidator pushQueryValidator
  ) {
    this.analyzer = requireNonNull(analyzer, "analyzer");
    this.pullQueryValidator = requireNonNull(pullQueryValidator, "pullQueryValidator");
    this.pushQueryValidator = requireNonNull(pushQueryValidator, "pushQueryValidator");
  }

  public Analysis analyze(
      final Query query,
      final Optional<Sink> sink
  ) {
    final Analysis analysis = analyzer.analyze(query, sink);

    if (query.isPullQuery()) {
      pullQueryValidator.validate(analysis);
    } else {
      pushQueryValidator.validate(analysis);
    }

    if (!analysis.getTableFunctions().isEmpty()) {
      final AliasedDataSource ds = analysis.getFrom();
      if (ds.getDataSource().getDataSourceType() == DataSourceType.KTABLE) {
        throw new KsqlException("Table source is not supported with table functions");
      }
    }

    return analysis;
  }
}
