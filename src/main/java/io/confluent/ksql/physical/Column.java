package io.confluent.ksql.physical;

import io.confluent.ksql.planner.types.Type;

public class Column {
    public final String name;
    public final Type type;

    public Column(String name, Type type) {
        this.name = name;
        this.type = type;
    }
}
