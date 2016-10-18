package io.confluent.ksql.planner.types;

public class StringType extends AbstractType {

    public static final StringType STRING = new StringType();

    private StringType()
    {
        super(String.class);
    }

    @Override
    public String getTypeName() {
        return "STRING";
    }
}
