package io.confluent.ksql.planner.types;

public class BooleanType extends AbstractType {

    public static final BooleanType BOOLEAN = new BooleanType();

    private BooleanType()
    {
        super(boolean.class);

    }

}
