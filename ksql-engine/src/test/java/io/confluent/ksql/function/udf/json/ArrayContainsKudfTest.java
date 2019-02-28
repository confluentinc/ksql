/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.json;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class ArrayContainsKudfTest
{
    private ArrayContainsKudf jsonUdf = new ArrayContainsKudf();

    @Test
    public void shouldReturnFalseOnEmptyArray() {
        assertEquals(false, jsonUdf.evaluate("[]", true));
        assertEquals(false, jsonUdf.evaluate("[]", false));
        assertEquals(false, jsonUdf.evaluate("[]", null));
        assertEquals(false, jsonUdf.evaluate("[]", 1.0));
        assertEquals(false, jsonUdf.evaluate("[]", 100));
        assertEquals(false, jsonUdf.evaluate("[]", "abc"));
        assertEquals(false, jsonUdf.evaluate("[]", ""));
    }

    @Test
    public void shouldNotFindValuesInNullArray() {
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
    public void shouldFindIntegersInJsonArray() {
        final String json = "[2147483647, {\"ab\":null }, -2147483648, 1, 2, 3, null, [4], 4]";
        assertEquals(true, jsonUdf.evaluate(json, 2147483647));
        assertEquals(true, jsonUdf.evaluate(json, -2147483648));
        assertEquals(true, jsonUdf.evaluate(json, 1));
        assertEquals(true, jsonUdf.evaluate(json, 2));
        assertEquals(true, jsonUdf.evaluate(json, 3));
        assertEquals(false, jsonUdf.evaluate("5", 5));
        assertEquals(false, jsonUdf.evaluate(json, 5));
    }

    @Test
    public void shouldFindLongsInJsonArray() {
        assertEquals(true, jsonUdf.evaluate("[1]", 1L));
        assertEquals(true, jsonUdf.evaluate("[1111111111111111]", 1111111111111111L));
        assertEquals(true, jsonUdf.evaluate("[[222222222222222], 33333]", 33333L));
        assertEquals(true, jsonUdf.evaluate("[{}, \"abc\", null, 1]", 1L));
        assertEquals(false, jsonUdf.evaluate("[[222222222222222], 33333]", 222222222222222L));
        assertEquals(false, jsonUdf.evaluate("[{}, \"abc\", null, [1]]", 1L));
        assertEquals(false, jsonUdf.evaluate("[{}, \"abc\", null, {\"1\":1}]", 1L));
    }

    @Test
    public void shouldFindDoublesInJsonArray() {
        assertEquals(true, jsonUdf.evaluate("[-1.0, 2.0, 3.0]", 2.0));
        assertEquals(true, jsonUdf.evaluate("[1.0, -2.0, 3.0]", -2.0));
        assertEquals(true, jsonUdf.evaluate("[1.0, 2.0, 1.6E3]", 1600.0));
        assertEquals(true, jsonUdf.evaluate("[1.0, 2.0, -1.6E3]", -1600.0));
        assertEquals(true, jsonUdf.evaluate("[{}, \"abc\", null, -2.0]", -2.0));
        assertEquals(false, jsonUdf.evaluate("[[2.0], 3.0]", 2.0));
    }

    @Test
    public void shouldFindStringsInJsonArray() {
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
    public void shouldFindBooleansInJsonArray() {
        assertEquals(true, jsonUdf.evaluate("[false, false, true, false]", true));
        assertEquals(true, jsonUdf.evaluate("[true, true, false]", false));
        assertEquals(false, jsonUdf.evaluate("[true, true]", false));
        assertEquals(false, jsonUdf.evaluate("[false, false]", true));
    }

    @Test
    public void shouldReturnFalseOnEmptyList() {
        assertEquals(false, jsonUdf.evaluate(Collections.emptyList(), true));
        assertEquals(false, jsonUdf.evaluate(Collections.emptyList(), false));
        assertEquals(false, jsonUdf.evaluate(Collections.emptyList(), null));
        assertEquals(false, jsonUdf.evaluate(Collections.emptyList(), 1.0));
        assertEquals(false, jsonUdf.evaluate(Collections.emptyList(), 100));
        assertEquals(false, jsonUdf.evaluate(Collections.emptyList(), "abc"));
        assertEquals(false, jsonUdf.evaluate(Collections.emptyList(), ""));
    }

    @Test
    public void shouldNotFindValuesInNullListElements() {
        assertEquals(true, jsonUdf.evaluate(Collections.singletonList(null), null));
        assertEquals(false, jsonUdf.evaluate(Collections.singletonList(null), "null"));
        assertEquals(false, jsonUdf.evaluate(Collections.singletonList(null), true));
        assertEquals(false, jsonUdf.evaluate(Collections.singletonList(null), false));
        assertEquals(false, jsonUdf.evaluate(Collections.singletonList(null), 1.0));
        assertEquals(false, jsonUdf.evaluate(Collections.singletonList(null), 100));
        assertEquals(false, jsonUdf.evaluate(Collections.singletonList(null), "abc"));
        assertEquals(false, jsonUdf.evaluate(Collections.singletonList(null), ""));
    }

    @Test
    public void shouldFindStringInList() {
        assertEquals(true, jsonUdf.evaluate(Arrays.asList("abc", "bd", "DC"), "DC"));
        assertEquals(false, jsonUdf.evaluate(Arrays.asList("abc", "bd", "DC"), "dc"));
        assertEquals(false, jsonUdf.evaluate(Arrays.asList("abc", "bd", "1"), 1));
    }

    @Test
    public void shouldFindIntegersInList() {
        assertEquals(true, jsonUdf.evaluate(Arrays.asList(1, 2, 3), 2));
        assertEquals(false, jsonUdf.evaluate(Arrays.asList(1, 2, 3), 0));
        assertEquals(false, jsonUdf.evaluate(Arrays.asList(1, 2, 3), "1"));
        assertEquals(false, jsonUdf.evaluate(Arrays.asList(1, 2, 3), "aa"));
    }

    @Test
    public void shouldFindLongInList() {
        assertEquals(true, jsonUdf.evaluate(Arrays.asList(1L, 2L, 3L), 2L));
        assertEquals(false, jsonUdf.evaluate(Arrays.asList(1L, 2L, 3L), 0L));
        assertEquals(false, jsonUdf.evaluate(Arrays.asList(1L, 2L, 3L), "1"));
        assertEquals(false, jsonUdf.evaluate(Arrays.asList(1L, 2L, 3L), "aaa"));
    }

    @Test
    public void shouldFindDoublesInList() {
        assertEquals(true, jsonUdf.evaluate(Arrays.asList(1.0, 2.0, 3.0), 2.0));
        assertEquals(false, jsonUdf.evaluate(Arrays.asList(1.0, 2.0, 3.0), 4.0));
        assertEquals(false, jsonUdf.evaluate(Arrays.asList(1.0, 2.0, 3.0), "1"));
        assertEquals(false, jsonUdf.evaluate(Arrays.asList(1.0, 2.0, 3.0), "aaa"));
    }
}