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
    public void testIntegerValues()
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

    public void testIfArrayContainsLongValue()
    {
        assertEquals(true, jsonUdf.evaluate("[1]", 1L));
        assertEquals(true, jsonUdf.evaluate("[1111111111111111]", 1111111111111111L));
        assertEquals(true, jsonUdf.evaluate("[[222222222222222], 33333]", 33333L));
        assertEquals(true, jsonUdf.evaluate("[{}, \"abc\", null, 1]", 1L));
        assertEquals(false, jsonUdf.evaluate("[[222222222222222], 33333]", 222222222222222L));
        assertEquals(false, jsonUdf.evaluate("[{}, \"abc\", null, [1]]", 1L));
        assertEquals(false, jsonUdf.evaluate("[{}, \"abc\", null, {\"1\":1}]", 1L));
    }

    public void testIfArrayContainsDoubleValue()
    {
        //TODO:
    }

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

    public void testIfArrayContainsBooleanValue()
    {
        assertEquals(true, jsonUdf.evaluate("[false, false, true, false]", true));
        assertEquals(true, jsonUdf.evaluate("[true, true, false]", false));
        assertEquals(false, jsonUdf.evaluate("[true, true]", false));
        assertEquals(false, jsonUdf.evaluate("[false, false]", true));

    }
}