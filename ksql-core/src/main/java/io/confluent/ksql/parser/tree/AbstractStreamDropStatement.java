/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.parser.tree;

import java.util.Optional;

public abstract class AbstractStreamDropStatement extends Statement {
    public AbstractStreamDropStatement(Optional<NodeLocation> location) {
        super(location);
    }

    public abstract QualifiedName getName();
}
