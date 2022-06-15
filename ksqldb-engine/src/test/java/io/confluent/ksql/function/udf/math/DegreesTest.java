package io.confluent.ksql.function.udf.math;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DegreesTest {
    private Degrees udf;

    @Before
    public void setUp() {
        udf = new Degrees();
    }

    @Test
    public void shouldHandleNull() {
        assertThat(udf.degrees((Integer)null), is(nullValue()));
        assertThat(udf.degrees((Long)null), is(nullValue()));
        assertThat(udf.degrees((Double)null), is(nullValue()));
    }

    @Test
    public void shouldHandleNegative() {
        assertThat(udf.degrees(-Math.PI), is(-180.0));
        assertThat(udf.degrees(-2 * Math.PI), is(-360.0));
        assertThat(udf.degrees(-1.2345), is(-70.73163980890013));
        assertThat(udf.degrees(-2), is(-114.59155902616465));
        assertThat(udf.degrees(-2L), is(-114.59155902616465));
    }

    @Test
    public void shouldHandleZero() {
        assertThat(udf.degrees(0), is(0.0));
        assertThat(udf.degrees(0L), is(0.0));
        assertThat(udf.degrees(0.0), is(0.0));
    }

    @Test
    public void shouldHandlePositive() {
        assertThat(udf.degrees(Math.PI), is(180.0));
        assertThat(udf.degrees(2 * Math.PI), is(360.0));
        assertThat(udf.degrees(1.2345), is(70.73163980890013));
        assertThat(udf.degrees(2), is(114.59155902616465));
        assertThat(udf.degrees(2L), is(114.59155902616465));
    }
}
