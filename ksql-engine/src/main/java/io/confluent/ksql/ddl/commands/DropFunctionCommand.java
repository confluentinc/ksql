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

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.DropFunction;
import io.confluent.ksql.util.KsqlException;


public class DropFunctionCommand implements DdlCommand {

  private final String functionName;
  private final Boolean isExists;

  public DropFunctionCommand(final DropFunction dropFunction) {
    this.functionName = dropFunction.getName();
    this.isExists = dropFunction.isExists();
  }

  DropFunctionCommand(final String functionName, final Boolean isExists) {
    this.functionName = functionName;
    this.isExists = isExists;
  }

  @Override
  public DdlCommandResult run(final MutableMetaStore metaStore) {
    final MutableFunctionRegistry functionRegistry = metaStore.getFunctionRegistry();
    try {
      if (isExists && !functionRegistry.isInline(functionName)) {
        functionRegistry.dropFunction(functionName);
        return new DdlCommandResult(true, "Function " + functionName + " does not exist");
      }
      functionRegistry.dropFunction(functionName);
      return new DdlCommandResult(true, "Function " + functionName + " was dropped");
    } catch (Exception e) {
      final String errorMessage =
            String.format("Cannot drop function '%s': %s",
                functionName, e.getMessage());
      throw new KsqlException(errorMessage, e);
    }
  }
}
