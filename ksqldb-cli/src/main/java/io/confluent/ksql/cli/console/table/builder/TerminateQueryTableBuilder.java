/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.cli.console.table.builder;

import io.confluent.ksql.cli.console.table.Table;
import io.confluent.ksql.rest.entity.TerminateQueryEntity;

/**
 * This builder is only used to terminate transient queries.
 */
public class TerminateQueryTableBuilder implements TableBuilder<TerminateQueryEntity> {

  @Override
  public Table buildTable(final TerminateQueryEntity entity) {
    return new Table.Builder()
        .withColumnHeaders("Message")
        .withRow("Terminated query \"" + entity.getQueryId() + '"')
        .build();
  }
}
