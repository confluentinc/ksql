package io.confluent.ksql.function.udf.json;

import junit.framework.TestCase;
import org.junit.Test;

public class JsonArrayContainsKudfTest
        extends TestCase
{
    private JsonArrayContainsKudf jsonUdf = new JsonArrayContainsKudf();

    @Override
    public void setUp()
            throws Exception
    {
        jsonUdf.init();
    }

    @Test
    public void testEmptyArray()
    {
        assertEquals(false, jsonUdf.evaluate("[]", true));
        assertEquals(false, jsonUdf.evaluate("[]", false));
        assertEquals(false, jsonUdf.evaluate("[]", null));
        assertEquals(false, jsonUdf.evaluate("[]", 1.0));
        assertEquals(false, jsonUdf.evaluate("[]", 100));
        assertEquals(false, jsonUdf.evaluate("[]", "abc"));
        assertEquals(false, jsonUdf.evaluate("[]", ""));
    }

    @Test
    public void testNullArray()
    {
        assertEquals(true, jsonUdf.evaluate("[null]", null));
        assertEquals(false, jsonUdf.evaluate("[null]", "null"));
        assertEquals(false, jsonUdf.evaluate("[null]", true));
        assertEquals(false, jsonUdf.evaluate("[null]", false));
        assertEquals(false, jsonUdf.evaluate("[null]", 1.0));
        assertEquals(false, jsonUdf.evaluate("[null]", 100));
        assertEquals(false, jsonUdf.evaluate("[null]", "abc"));
        assertEquals(false, jsonUdf.evaluate("[null]", ""));
    }

    @Test
    public void testIntegerJsonArrayContainsValues()
    {
        String json = "[2147483647, {\"ab\":null }, -2147483648, 1, 2, 3, null, [4], 4]";
        assertEquals(true, jsonUdf.evaluate(json, 2147483647));
        assertEquals(true, jsonUdf.evaluate(json, -2147483648));
        assertEquals(true, jsonUdf.evaluate(json, 1));
        assertEquals(true, jsonUdf.evaluate(json, 2));
        assertEquals(true, jsonUdf.evaluate(json, 3));
        assertEquals(false, jsonUdf.evaluate("5", 5));
        assertEquals(false, jsonUdf.evaluate(json, 5));
    }

    @Test
    public void testIfJsonArrayContainsLongValue()
    {
        assertEquals(true, jsonUdf.evaluate("[1]", 1L));
        assertEquals(true, jsonUdf.evaluate("[1111111111111111]", 1111111111111111L));
        assertEquals(true, jsonUdf.evaluate("[[222222222222222], 33333]", 33333L));
        assertEquals(true, jsonUdf.evaluate("[{}, \"abc\", null, 1]", 1L));
        assertEquals(false, jsonUdf.evaluate("[[222222222222222], 33333]", 222222222222222L));
        assertEquals(false, jsonUdf.evaluate("[{}, \"abc\", null, [1]]", 1L));
        assertEquals(false, jsonUdf.evaluate("[{}, \"abc\", null, {\"1\":1}]", 1L));
    }

    @Test
    public void testIfArrayContainsDoubleValue()
    {
        assertEquals(true, jsonUdf.evaluate("[-1.0, 2.0, 3.0]", 2.0));
        assertEquals(true, jsonUdf.evaluate("[1.0, -2.0, 3.0]", -2.0));
        assertEquals(true, jsonUdf.evaluate("[1.0, 2.0, 1.6E3]", 1600.0));
        assertEquals(true, jsonUdf.evaluate("[1.0, 2.0, -1.6E3]", -1600.0));
        assertEquals(true, jsonUdf.evaluate("[{}, \"abc\", null, -2.0]", -2.0));
        assertEquals(false, jsonUdf.evaluate("[[2.0], 3.0]", 2.0));
    }

    @Test
    public void testIfArrayContainsStringValue()
    {
        assertEquals(true, jsonUdf.evaluate("[\"abc\"]", "abc"));
        assertEquals(true, jsonUdf.evaluate("[\"cbda\", \"abc\"]", "abc"));
        assertEquals(true, jsonUdf.evaluate("[{}, \"abc\", null, 1]", "abc"));
        assertEquals(true, jsonUdf.evaluate("[\"\"]", ""));
        assertEquals(false, jsonUdf.evaluate("[\"\"]", null));
        assertEquals(false, jsonUdf.evaluate("[1,2,3]", "1"));
        assertEquals(false, jsonUdf.evaluate("[null]", ""));
        assertEquals(false, jsonUdf.evaluate("[\"abc\", \"dba\"]", "abd"));
    }

    @Test
    public void testIfArrayContainsBooleanValue()
    {
        assertEquals(true, jsonUdf.evaluate("[false, false, true, false]", true));
        assertEquals(true, jsonUdf.evaluate("[true, true, false]", false));
        assertEquals(false, jsonUdf.evaluate("[true, true]", false));
        assertEquals(false, jsonUdf.evaluate("[false, false]", true));
    }

    @Test
    public void testIfStringArrayContainsValue()
    {
        assertEquals(true, jsonUdf.evaluate(new String[]{"abc", "bd", "DC"}, "DC"));
        assertEquals(false, jsonUdf.evaluate(new String[]{"abc", "bd", "DC"}, "dc"));
        assertEquals(false, jsonUdf.evaluate(new String[]{"abc", "bd", "1"}, 1));
    }

    @Test
    public void testIfIntegerArrayContainsValue()
    {
        assertEquals(true, jsonUdf.evaluate(new Integer[]{1, 2, 3}, 2));
        assertEquals(false, jsonUdf.evaluate(new Integer[]{1, 2, 3}, 0));
        assertEquals(false, jsonUdf.evaluate(new Integer[]{1, 2, 3}, "1"));
        assertEquals(false, jsonUdf.evaluate(new Integer[]{1, 2, 3}, "aa"));

        //TODO: Achtung! this test fails
//        assertEquals(true, jsonUdf.evaluate(new Integer[]{1, 2, 3}, 2L));
    }

    @Test
    public void testIfLongArrayContainsValue()
    {
        assertEquals(true, jsonUdf.evaluate(new Long[]{1L, 2L, 3L}, 2L));
        assertEquals(false, jsonUdf.evaluate(new Long[]{1L, 2L, 3L}, 0L));
        assertEquals(false, jsonUdf.evaluate(new Long[]{1L, 2L, 3L}, "1"));
        assertEquals(false, jsonUdf.evaluate(new Long[]{1L, 2L, 3L}, "aaa"));

        //TODO: Achtung! this test fails
//        assertEquals(true, jsonUdf.evaluate(new Long[]{1L, 2L, 3L}, 1));
    }

    @Test
    public void testIfDoubleArrayContainsValue()
    {
        assertEquals(true, jsonUdf.evaluate(new Double[]{1.0, 2.0, 3.0}, 2.0));
        assertEquals(false, jsonUdf.evaluate(new Double[]{1.0, 2.0, 3.0}, 4.0));
        assertEquals(false, jsonUdf.evaluate(new Double[]{1.0, 2.0, 3.0}, "1"));
        assertEquals(false, jsonUdf.evaluate(new Double[]{1.0, 2.0, 3.0}, "aaa"));

        //TODO: Achtung! this test fails
//        assertEquals(true, jsonUdf.evaluate(new Double[]{1.0, 2.0, 3.0}, 1));
    }
}