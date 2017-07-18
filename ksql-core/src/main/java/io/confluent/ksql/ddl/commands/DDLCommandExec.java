/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.metastore.MetaStore;

/**
 * Execute DDL Commands
 */
public class DDLCommandExec {

  public DDLCommandResult execute(DDLCommand ddlCommand, MetaStore metaStore) {
    // TODO: create new task to run
    return ddlCommand.run(metaStore);
  }
}
