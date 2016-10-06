package io.confluent.ksql.planner.types;

public abstract class AbstractType implements Type {

    private final Class<?> javaType;

    protected AbstractType(Class<?> javaType) {
        this.javaType = javaType;
    }

    @Override
    public final Class<?> getJavaType()
    {
        return javaType;
    }

    @Override
    public boolean isComparable()
    {
        return false;
    }

    @Override
    public boolean isOrderable()
    {
        return false;
    }

}
