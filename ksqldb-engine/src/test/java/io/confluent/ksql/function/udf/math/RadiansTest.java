package io.confluent.ksql.function.udf.math;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RadiansTest {
    private Radians udf;

    @Before
    public void setUp() {
        udf = new Radians();
    }

    @Test
    public void shouldHandleNull() {
        assertThat(udf.radians((Integer)null), is(nullValue()));
        assertThat(udf.radians((Long)null), is(nullValue()));
        assertThat(udf.radians((Double)null), is(nullValue()));
    }

    @Test
    public void shouldHandleNegative() {
        assertThat(udf.radians(-180.0), is(-Math.PI));
        assertThat(udf.radians(-360.0), is(-2 * Math.PI));
        assertThat(udf.radians(-70.73163980890013), is(-1.2345));
        assertThat(udf.radians(-114), is(-1.9896753472735358));
        assertThat(udf.radians(-114L), is(-1.9896753472735358));
    }

    @Test
    public void shouldHandleZero() {
        assertThat(udf.radians(0), is(0.0));
        assertThat(udf.radians(0L), is(0.0));
        assertThat(udf.radians(0.0), is(0.0));
    }

    @Test
    public void shouldHandlePositive() {
        assertThat(udf.radians(180.0), is(Math.PI));
        assertThat(udf.radians(360.0), is(2 * Math.PI));
        assertThat(udf.radians(70.73163980890013), is(1.2345));
        assertThat(udf.radians(114), is(1.9896753472735358));
        assertThat(udf.radians(114L), is(1.9896753472735358));
    }
}
