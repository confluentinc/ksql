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

package io.confluent.ksql.rest.server.execution;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.metastore.TypeRegistry.CustomType;
import io.confluent.ksql.parser.tree.ListTypes;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.SchemaInfo;
import io.confluent.ksql.rest.entity.TypeList;
import io.confluent.ksql.rest.util.EntityUtil;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.Iterator;
import java.util.Optional;

public final class ListTypesExecutor {

  private ListTypesExecutor() {

  }

  public static StatementExecutorResponse execute(
      final ConfiguredStatement<ListTypes> configuredStatement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final ImmutableMap.Builder<String, SchemaInfo> types = ImmutableMap.builder();

    final Iterator<CustomType> customTypes = executionContext.getMetaStore().types();
    while (customTypes.hasNext()) {
      final CustomType customType = customTypes.next();
      types.put(customType.getName(), EntityUtil.schemaInfo(customType.getType()));
    }

    return StatementExecutorResponse.handled(Optional.of(
        new TypeList(configuredStatement.getMaskedStatementText(), types.build())));
  }
}
