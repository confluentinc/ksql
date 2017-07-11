/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.physical.GenericRow;

import java.util.List;

public interface DDLCommand {

    List<GenericRow> run(MetaStore metaStore);

}
