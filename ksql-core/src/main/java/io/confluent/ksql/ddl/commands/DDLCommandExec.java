/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.exception.ExceptionUtil;
import io.confluent.ksql.util.KsqlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * @param ddlCommand
   * @return
   */
  public DDLCommandResult tryExecute(DDLCommand ddlCommand, MetaStore tempMetaStore) {
    if (tempMetaStore == metaStore) {
      throw new KsqlException("Try to execute DDLCommand on tempMetaStore, but getting the real MetaStore.");
    }
    return executeOnMetaStore(ddlCommand, tempMetaStore);
  }

  /**
   * execute on real metaStore
   * @param ddlCommand
   * @return
   */
  public DDLCommandResult execute(DDLCommand ddlCommand) {
    return executeOnMetaStore(ddlCommand, this.metaStore);
  }

  private static DDLCommandResult executeOnMetaStore(DDLCommand ddlCommand, MetaStore metaStore) {
    // TODO: create new task to run
    try {
      return ddlCommand.run(metaStore);
    } catch (Exception e) {
      String stackTrace = ExceptionUtil.stackTraceToString(e);
      LOGGER.error(stackTrace);
      return new DDLCommandResult(false, stackTrace);
    }
  }
}
