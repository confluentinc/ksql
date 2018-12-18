/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlReferentialIntegrityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execute DDL Commands
 */
public class DdlCommandExec {

  private static final Logger LOGGER = LoggerFactory.getLogger(DdlCommandExec.class);
  private final MetaStore metaStore;

  public DdlCommandExec(final MetaStore metaStore) {
    this.metaStore = metaStore;
  }

  /**
   * execute on temp metaStore
   */
  public DdlCommandResult tryExecute(final DdlCommand ddlCommand, final MetaStore tempMetaStore) {
    if (tempMetaStore == metaStore) {
      throw new KsqlException(
          "Try to execute DDLCommand on tempMetaStore, but getting the real MetaStore."
      );
    }
    return executeOnMetaStore(ddlCommand, tempMetaStore, true);
  }

  /**
   * execute on real metaStore
   */
  public DdlCommandResult execute(final DdlCommand ddlCommand, final boolean isValidatePhase) {
    return executeOnMetaStore(ddlCommand, this.metaStore, isValidatePhase);
  }

  private static DdlCommandResult executeOnMetaStore(
      final DdlCommand ddlCommand,
      final MetaStore metaStore,
      final boolean isValidatePhase) {
    // TODO: create new task to run
    try {
      return ddlCommand.run(metaStore, isValidatePhase);
    } catch (final KsqlReferentialIntegrityException referentialIntegrityException) {
      LOGGER.warn(String.format("executeOnMetaStore:%s", ddlCommand),
                  referentialIntegrityException);
      throw referentialIntegrityException;
    }
  }
}
