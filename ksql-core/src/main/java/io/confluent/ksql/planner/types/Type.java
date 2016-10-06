package io.confluent.ksql.planner.types;

public interface Type {


    /**
     * True if the type supports equalTo and hash.
     */
    boolean isComparable();

    /**
     * True if the type supports compareTo.
     */
    boolean isOrderable();

    /**
     * Gets the Java class type used to represent this value on the stack during
     * expression execution. This value is used to determine which method should
     * be called on Cursor, RecordSet or RandomAccessBlock to fetch a value of
     * this type.
     * <p>
     * Currently, this must be boolean, int, long, double or String.
     */
    Class<?> getJavaType();

}
