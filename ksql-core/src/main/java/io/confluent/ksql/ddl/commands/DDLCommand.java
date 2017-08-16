/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.metastore.MetaStore;

public interface DDLCommand {

  DDLCommandResult run(MetaStore metaStore);

}
