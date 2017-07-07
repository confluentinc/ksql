/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.AbstractStreamDropStatement;
import io.confluent.ksql.physical.GenericRow;

import java.util.ArrayList;
import java.util.List;

public class DropSourceCommand implements DDLCommand {

    private final String sourceName;

    public DropSourceCommand(AbstractStreamDropStatement statement) {
        this.sourceName = statement.getName().getSuffix();
    }

    @Override
    public List<GenericRow> run(MetaStore metaStore) {
        metaStore.deleteSource(sourceName);
        return new ArrayList<>();
    }
}
