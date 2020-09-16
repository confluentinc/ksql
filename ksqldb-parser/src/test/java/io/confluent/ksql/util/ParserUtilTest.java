package io.confluent.ksql.util;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class ParserUtilTest {

    @Test
    public void shouldBeReserved() {
        // Given:
        final String[] keywords = new String[]{
                "size",      // reserved word
                "load",      // reserved word
                "SIZE",      //upper case
                "Load"       //case insensitive
        };

        // Then:
        for (final String keyword : keywords) {
            assertEquals(true, ParserUtil.isReserved(keyword));
        }
    }

    @Test
    public void shouldNotBeReserved() {
        // Given:
        final String[] keywords = new String[]{
                "source",      // non-reserved keyword
                "sink",        // non-reserved keyword
                "MAP",         //upper case
                "Array",        //case insensitive
                "ASSERT",
                "foo",
                "bAR"
        };

        // Then:
        for (final String keyword : keywords) {
            assertEquals(false, ParserUtil.isReserved(keyword));
        }
    }
}
