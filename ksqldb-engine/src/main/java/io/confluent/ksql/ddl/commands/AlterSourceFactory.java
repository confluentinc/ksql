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

package io.confluent.ksql.ddl.commands;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.execution.ddl.commands.AlterSourceCommand;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.tree.AlterSource;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import java.util.List;
import java.util.stream.Collectors;

public class AlterSourceFactory {
  @VisibleForTesting
  AlterSourceFactory() {
  }

  public AlterSourceCommand create(final AlterSource statement) {
    final List<Column> newColumns = statement
        .getAlterOptions()
        .stream()
        .map(
            alterOption -> Column.of(
                ColumnName.of(alterOption.getColumnName()),
                alterOption.getType().getSqlType(),
                Namespace.VALUE,
                0))
        .collect(Collectors.toList());

    return new AlterSourceCommand(
        statement.getName(),
        statement.getDataSourceType().getKsqlType(),
        newColumns
    );
  }
}
