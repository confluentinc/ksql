package io.confluent.ksql.function.udf.math;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class PiTest {
    private Pi udf;

    @Before
    public void setUp() {
        udf = new Pi();
    }

    @Test
    public void shouldReturnPi() {
        assertThat(udf.pi(), is(Math.PI));
    }
}
