package io.confluent.ksql.function.udf.math;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class AtanTest {
    private Atan udf;

    @Before
    public void setUp() {
        udf = new Atan();
    }

    @Test
    public void shouldHandleNull() {
        assertThat(udf.atan((Integer)null), is(nullValue()));
        assertThat(udf.atan((Long)null), is(nullValue()));
        assertThat(udf.atan((Double)null), is(nullValue()));
    }

    @Test
    public void shouldHandleLessThanNegativeOne() {
        assertThat(udf.atan(-1.1), is(-0.8329812666744317));
        assertThat(udf.atan(-6.0), is(-1.4056476493802699));
        assertThat(udf.atan(-2), is(-1.1071487177940904));
        assertThat(udf.atan(-2L), is(-1.1071487177940904));
    }

    @Test
    public void shouldHandleNegative() {
        assertThat(udf.atan(-0.43), is(-0.40609805831761564));
        assertThat(udf.atan(-0.5), is(-0.4636476090008061));
        assertThat(udf.atan(-1.0), is(-0.7853981633974483));
        assertThat(udf.atan(-1), is(-0.7853981633974483));
        assertThat(udf.atan(-1L), is(-0.7853981633974483));
    }

    @Test
    public void shouldHandleZero() {
        assertThat(udf.atan(0.0), is(0.0));
        assertThat(udf.atan(0), is(0.0));
        assertThat(udf.atan(0L), is(0.0));
    }

    @Test
    public void shouldHandlePositive() {
        assertThat(udf.atan(0.43), is(0.40609805831761564));
        assertThat(udf.atan(0.5), is(0.4636476090008061));
        assertThat(udf.atan(1.0), is(0.7853981633974483));
        assertThat(udf.atan(1), is(0.7853981633974483));
        assertThat(udf.atan(1L), is(0.7853981633974483));
    }

    @Test
    public void shouldHandleMoreThanPositiveOne() {
        assertThat(udf.atan(1.1), is(0.8329812666744317));
        assertThat(udf.atan(6.0), is(1.4056476493802699));
        assertThat(udf.atan(2), is(1.1071487177940904));
        assertThat(udf.atan(2L), is(1.1071487177940904));
    }
}
