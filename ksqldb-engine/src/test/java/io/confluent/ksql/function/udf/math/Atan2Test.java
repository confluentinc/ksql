package io.confluent.ksql.function.udf.math;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class Atan2Test {
    private Atan2 udf;

    @Before
    public void setUp() {
        udf = new Atan2();
    }

    @Test
    public void shouldHandleNull() {
        assertThat(udf.atan2(null, 1), is(nullValue()));
        assertThat(udf.atan2(null, 1L), is(nullValue()));
        assertThat(udf.atan2(null, 0.45), is(nullValue()));
        assertThat(udf.atan2(1, null), is(nullValue()));
        assertThat(udf.atan2(1L, null), is(nullValue()));
        assertThat(udf.atan2(0.45, null), is(nullValue()));
        assertThat(udf.atan2((Integer)null, null), is(nullValue()));
        assertThat(udf.atan2((Long)null, null), is(nullValue()));
        assertThat(udf.atan2((Double)null, null), is(nullValue()));
    }

    @Test
    public void shouldHandleNegativeXNegativeY() {
        assertThat(udf.atan2(-1.1, -0.24), is(-1.7856117271965553));
        assertThat(udf.atan2(-6.0, -7.1), is(-2.4399674339361113));
        assertThat(udf.atan2(-2, -3), is(-2.5535900500422257));
        assertThat(udf.atan2(-2L, -2L), is(-2.356194490192345));
    }

    @Test
    public void shouldHandleNegativeXPositiveY() {
        assertThat(udf.atan2(-1.1, 0.24), is(-1.355980926393238));
        assertThat(udf.atan2(-6.0, 7.1), is(-0.7016252196536817));
        assertThat(udf.atan2(-2, 3), is(-0.5880026035475675));
        assertThat(udf.atan2(-2L, 2L), is(-0.7853981633974483));
    }

    @Test
    public void shouldHandleNegativeXZeroY() {
        assertThat(udf.atan2(-1.1, 0.0), is(-1.5707963267948966));
        assertThat(udf.atan2(-6.0, 0.0), is(-1.5707963267948966));
        assertThat(udf.atan2(-2, 0), is(-1.5707963267948966));
        assertThat(udf.atan2(-2L, 0L), is(-1.5707963267948966));
    }

    @Test
    public void shouldHandleZeroXNegativeY() {
        assertThat(udf.atan2(0.0, -0.24), is(3.141592653589793));
        assertThat(udf.atan2(0.0, -7.1), is(3.141592653589793));
        assertThat(udf.atan2(0, -3), is(3.141592653589793));
        assertThat(udf.atan2(0L, -2L), is(3.141592653589793));
    }

    @Test
    public void shouldHandleZeroXPositiveY() {
        assertThat(udf.atan2(0.0, 0.24), is(0.0));
        assertThat(udf.atan2(0.0, 7.1), is(0.0));
        assertThat(udf.atan2(0, 3), is(0.0));
        assertThat(udf.atan2(0L, 2L), is(0.0));
    }

    @Test
    public void shouldHandleZeroXZeroY() {
        assertThat(udf.atan2(0.0, 0.0), is(0.0));
        assertThat(udf.atan2(0.0, 0.0), is(0.0));
        assertThat(udf.atan2(0, 0), is(0.0));
        assertThat(udf.atan2(0L, 0L), is(0.0));
    }

    @Test
    public void shouldHandlePositiveXNegativeY() {
        assertThat(udf.atan2(1.1, -0.24), is(1.7856117271965553));
        assertThat(udf.atan2(6.0, -7.1), is(2.4399674339361113));
        assertThat(udf.atan2(2, -3), is(2.5535900500422257));
        assertThat(udf.atan2(2L, -2L), is(2.356194490192345));
    }

    @Test
    public void shouldHandlePositiveXPositiveY() {
        assertThat(udf.atan2(1.1, 0.24), is(1.355980926393238));
        assertThat(udf.atan2(6.0, 7.1), is(0.7016252196536817));
        assertThat(udf.atan2(2, 3), is(0.5880026035475675));
        assertThat(udf.atan2(2L, 2L), is(0.7853981633974483));
    }

    @Test
    public void shouldHandlePositiveXZeroY() {
        assertThat(udf.atan2(1.1, 0.0), is(1.5707963267948966));
        assertThat(udf.atan2(6.0, 0.0), is(1.5707963267948966));
        assertThat(udf.atan2(2, 0), is(1.5707963267948966));
        assertThat(udf.atan2(2L, 0L), is(1.5707963267948966));
    }
}
