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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.cli.console.table.Table;
import io.confluent.ksql.rest.entity.ConnectorPluginsList;
import io.confluent.ksql.rest.entity.SimpleConnectorPluginInfo.PluginType;
import java.util.List;
import org.apache.commons.lang3.ObjectUtils;

public class ConnectorPluginsListTableBuilder implements TableBuilder<ConnectorPluginsList> {
  private static final List<String> HEADERS = ImmutableList.of(
      "Class", "Type", "Version"
  );

  @Override
  public Table buildTable(final ConnectorPluginsList entity) {
    return new Table.Builder()
      .withColumnHeaders(HEADERS)
      .withRows(entity.getConnectorsPlugins()
            .stream()
            .map(info -> ImmutableList.of(
                info.getClassName(),
                ObjectUtils.defaultIfNull(info.getType(), PluginType.UNKNOWN).name(),
                ObjectUtils.defaultIfNull(info.getVersion(), ""))))
      .build();
  }
}
