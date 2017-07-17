/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.util.KsqlPreconditions;
import io.confluent.ksql.util.StringUtil;

import java.util.Map;

public class CreateTableCommand extends AbstractCreateStreamCommand {

  String stateStoreName;

  public CreateTableCommand(CreateTable createTable) {
    super(createTable);

    Map<String, Expression> properties = createTable.getProperties();

    checkStateStoreNameNotNull(properties);

    this.stateStoreName =  StringUtil.cleanQuotes(properties.get(DdlConfig.STATE_STORE_NAME_PROPERTY).toString());

  }

  @Override
  public DDLCommandResult run(MetaStore metaStore) {
    checkMetaData(metaStore, sourceName, topicName);

    KsqlTable ksqlTable = new KsqlTable(sourceName, schema,
        (keyColumnName.length() == 0) ? null :
            schema.field(keyColumnName),
        (timestampColumnName.length() == 0) ? null :
            schema.field(timestampColumnName),
        metaStore.getTopic(topicName),
        stateStoreName, isWindowed);

    // TODO: Need to check if the topic exists.
    // Add the topic to the metastore
    metaStore.putSource(ksqlTable.cloneWithTimeKeyColumns());
    return new DDLCommandResult(true, "Table created");
  }

  private void checkStateStoreNameNotNull(Map<String, Expression> properties) {
    KsqlPreconditions.checkNotNull(
        properties.get(DdlConfig.STATE_STORE_NAME_PROPERTY),
        "State store name for the table should be set in WITH clause.");
  }
}
