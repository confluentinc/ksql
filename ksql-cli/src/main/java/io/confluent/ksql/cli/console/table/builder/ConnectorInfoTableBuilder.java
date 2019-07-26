/*
 * Copyright 2019 Confluent Inc.
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
import io.confluent.ksql.rest.entity.ConnectorInfoEntity;
import io.confluent.ksql.rest.entity.ConnectorInfoEntity.Action;

public class ConnectorInfoTableBuilder implements TableBuilder<ConnectorInfoEntity> {

  @Override
  public Table buildTable(final ConnectorInfoEntity entity) {
    if (entity.getAction() == Action.CREATE) {
      return new Table.Builder()
          .withColumnHeaders("Message")
          .withRow("Created connector " + entity.getInfo().name())
          .build();
    }

    throw new UnsupportedOperationException("Cannot format ConnectorInfoEntity with action:"
        + entity.getAction());
  }
}
