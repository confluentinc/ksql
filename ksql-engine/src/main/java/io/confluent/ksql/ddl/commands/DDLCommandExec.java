/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.ddl.commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.util.KsqlException;

/**
 * Execute DDL Commands
 */
public class DDLCommandExec {

  private static final Logger LOGGER = LoggerFactory.getLogger(DDLCommandExec.class);
  private final MetaStore metaStore;

  public DDLCommandExec(MetaStore metaStore) {
    this.metaStore = metaStore;
  }

  /**
   * execute on temp metaStore
   */
  public DDLCommandResult tryExecute(DDLCommand ddlCommand, MetaStore tempMetaStore) {
    if (tempMetaStore == metaStore) {
      throw new KsqlException(
          "Try to execute DDLCommand on tempMetaStore, but getting the real MetaStore."
      );
    }
    return executeOnMetaStore(ddlCommand, tempMetaStore);
  }

  /**
   * execute on real metaStore
   */
  public DDLCommandResult execute(DDLCommand ddlCommand) {
    return executeOnMetaStore(ddlCommand, this.metaStore);
  }

  private static DDLCommandResult executeOnMetaStore(DDLCommand ddlCommand, MetaStore metaStore) {
    // TODO: create new task to run
    try {
      return ddlCommand.run(metaStore);
    } catch (Exception e) {
      LOGGER.warn(String.format("executeOnMetaStore:%s", ddlCommand), e);
      return new DDLCommandResult(false, e.getMessage());
    }
  }
}
