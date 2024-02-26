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

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.execution.ddl.commands.DropSourceCommand;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

public final class DropSourceFactory {
  private final MetaStore metaStore;

  DropSourceFactory(final MetaStore metaStore) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
  }

  public DropSourceCommand create(final DropStream statement) {
    return create(
        statement.getName(),
        statement.getIfExists(),
        statement.isDeleteTopic(),
        DataSourceType.KSTREAM
    );
  }

  public DropSourceCommand create(final DropTable statement) {
    return create(
        statement.getName(),
        statement.getIfExists(),
        statement.isDeleteTopic(),
        DataSourceType.KTABLE
    );
  }

  private DropSourceCommand create(
      final SourceName sourceName,
      final boolean ifExists,
      final boolean deleteTopic,
      final DataSourceType dataSourceType) {
    final DataSource dataSource = metaStore.getSource(sourceName);
    if (dataSource == null) {
      if (!ifExists) {
        final String hint = metaStore.checkAlternatives(sourceName, Optional.of(dataSourceType));
        throw new KsqlException(StringUtils.capitalize(dataSourceType.getKsqlType().toLowerCase())
            + " " + sourceName.text() + " does not exist." + hint);
      }
    } else if (dataSource.getDataSourceType() != dataSourceType) {
      throw new KsqlException(String.format(
          "Incompatible data source type is %s, but statement was DROP %s",
          dataSource.getDataSourceType().getKsqlType().toLowerCase(),
          dataSourceType.getKsqlType().toLowerCase()
      ));
    } else if (dataSource.isSource() && deleteTopic) {
      throw new KsqlException("Cannot delete topic for read-only source: " + sourceName.text());
    }
    return new DropSourceCommand(sourceName);
  }
}
