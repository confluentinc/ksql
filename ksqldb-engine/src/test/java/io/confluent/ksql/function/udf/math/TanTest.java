package io.confluent.ksql.function.udf.math;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.number.IsCloseTo.closeTo;

public class TanTest {
    private Tan udf;

    @Before
    public void setUp() {
        udf = new Tan();
    }

    @Test
    public void shouldHandleNull() {
        assertThat(udf.tan((Integer)null), is(nullValue()));
        assertThat(udf.tan((Long)null), is(nullValue()));
        assertThat(udf.tan((Double)null), is(nullValue()));
    }

    @Test
    public void shouldHandleLessThanNegative2Pi() {
        assertThat(udf.tan(-9.1), is(0.33670052643287396));
        assertThat(udf.tan(-6.3), is(-0.016816277694182057));
        assertThat(udf.tan(-7), is(-0.8714479827243188));
        assertThat(udf.tan(-7L), is(-0.8714479827243188));
    }

    @Test
    public void shouldHandleNegative() {
        assertThat(udf.tan(-0.43), is(-0.45862102348555517));
        assertThat(udf.tan(-Math.PI), closeTo(0, 0.000000000000001));
        assertThat(udf.tan(-Math.PI * 2), closeTo(0, 0.000000000000001));
        assertThat(udf.tan(-6), is(0.29100619138474915));
        assertThat(udf.tan(-6L), is(0.29100619138474915));
    }

    @Test
    public void shouldHandleZero() {
        assertThat(udf.tan(0.0), is(0.0));
        assertThat(udf.tan(0), is(0.0));
        assertThat(udf.tan(0L), is(0.0));
    }

    @Test
    public void shouldHandlePositive() {
        assertThat(udf.tan(0.43), is(0.45862102348555517));
        assertThat(udf.tan(Math.PI), closeTo(0, 0.000000000000001));
        assertThat(udf.tan(Math.PI * 2), closeTo(0, 0.000000000000001));
        assertThat(udf.tan(6), is(-0.29100619138474915));
        assertThat(udf.tan(6L), is(-0.29100619138474915));
    }

    @Test
    public void shouldHandleMoreThanPositive2Pi() {
        assertThat(udf.tan(9.1), is(-0.33670052643287396));
        assertThat(udf.tan(6.3), is(0.016816277694182057));
        assertThat(udf.tan(7), is(0.8714479827243188));
        assertThat(udf.tan(7L), is(0.8714479827243188));
    }
}
