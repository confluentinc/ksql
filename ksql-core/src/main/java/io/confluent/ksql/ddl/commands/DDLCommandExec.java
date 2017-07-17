/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.metastore.MetaStore;

/**
 * Execute DDL Commands
 */
public class DDLCommandExec {
  private final MetaStore metaStore;

  public DDLCommandExec(MetaStore metaStore) {
    this.metaStore = metaStore;
  }

  public DDLCommandResult execute(DDLCommand ddlCommand) {
    // TODO: create new task to run
    return ddlCommand.run(metaStore);
  }
}
