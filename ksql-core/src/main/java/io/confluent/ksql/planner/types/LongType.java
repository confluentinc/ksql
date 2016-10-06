package io.confluent.ksql.planner.types;

public class LongType extends AbstractType {

    public static final LongType LONG = new LongType();

    private LongType()
    {
        super(long.class);
    }
}
