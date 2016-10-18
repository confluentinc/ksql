package io.confluent.ksql.planner.types;

public class DoubleType extends AbstractType {

    public static final DoubleType DOUBLE = new DoubleType();

    private DoubleType()
    {
        super(double.class);
    }

    @Override
    public String getTypeName() {
        return "DOUBLE";
    }
}
