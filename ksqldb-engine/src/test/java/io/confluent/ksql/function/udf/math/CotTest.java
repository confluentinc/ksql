package io.confluent.ksql.function.udf.math;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class CotTest {
    private Cot udf;

    @Before
    public void setUp() {
        udf = new Cot();
    }

    @Test
    public void shouldHandleNull() {
        assertThat(udf.cot((Integer)null), is(nullValue()));
        assertThat(udf.cot((Long)null), is(nullValue()));
        assertThat(udf.cot((Double)null), is(nullValue()));
    }

    @Test
    public void shouldHandleLessThanNegative2Pi() {
        assertThat(udf.cot(-9.1), is(2.9699983263892054));
        assertThat(udf.cot(-6.3), is(-59.46619211372627));
        assertThat(udf.cot(-7), is(-1.1475154224051356));
        assertThat(udf.cot(-7L), is(-1.1475154224051356));
    }

    @Test
    public void shouldHandleNegative() {
        assertThat(udf.cot(-0.43), is(-2.1804495406685085));
        assertThat(udf.cot(-Math.PI), is(8.165619676597685E15));
        assertThat(udf.cot(-Math.PI * 2), is(4.0828098382988425E15));
        assertThat(udf.cot(-6), is(3.436353004180128));
        assertThat(udf.cot(-6L), is(3.436353004180128));
    }

    @Test
    public void shouldHandleZero() {
        assertThat(Double.isInfinite(udf.cot(0.0)), is(true));
        assertThat(Double.isInfinite(udf.cot(0)), is(true));
        assertThat(Double.isInfinite(udf.cot(0L)), is(true));
    }

    @Test
    public void shouldHandlePositive() {
        assertThat(udf.cot(0.43), is(2.1804495406685085));
        assertThat(udf.cot(Math.PI), is(-8.165619676597685E15));
        assertThat(udf.cot(Math.PI * 2), is(-4.0828098382988425E15));
        assertThat(udf.cot(6), is(-3.436353004180128));
        assertThat(udf.cot(6L), is(-3.436353004180128));
    }

    @Test
    public void shouldHandleMoreThanPositive2Pi() {
        assertThat(udf.cot(9.1), is(-2.9699983263892054));
        assertThat(udf.cot(6.3), is(59.46619211372627));
        assertThat(udf.cot(7), is(1.1475154224051356));
        assertThat(udf.cot(7L), is(1.1475154224051356));
    }
}
