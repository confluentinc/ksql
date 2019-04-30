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
  private final Object lock = new Object();

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
      // make this check + drop operation atomic with a synchonized lock
      synchronized (lock) {
        if (!functionRegistry.isInline(functionName)) {
          if (isExists) {
            // function doesn't exist, but IF EXISTS clause was specified, so no big deal
            return new DdlCommandResult(true, functionName + " is not an inline function");
          }
          // function doesn't exist, but IF EXISTS clause was NOT specified. throw an exception
          throw new KsqlException(functionName + " is not an inline function");
        }
      }
      // function is inline, do drop it
      functionRegistry.dropFunction(functionName);
      return new DdlCommandResult(true, "Function " + functionName + " was dropped");
    } catch (Exception e) {
      final String errorMessage =
            String.format("Cannot drop function: %s", e.getMessage());
      throw new KsqlException(errorMessage, e);
    }
  }
}

