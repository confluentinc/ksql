package io.confluent.ksql.planner.types;

public class IntegerType extends AbstractType {

    public static final IntegerType INTEGER = new IntegerType();

    private IntegerType()
    {
        super(int.class);
    }
}
