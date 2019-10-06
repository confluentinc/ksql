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

import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

public class ArrayContainsTest
{
    private ArrayContains jsonUdf = new ArrayContains();

    @Test
    public void shouldReturnFalseOnEmptyArray() {
        assertEquals(false, jsonUdf.arrayContains("[]", true));
        assertEquals(false, jsonUdf.arrayContains("[]", false));
        assertEquals(false, jsonUdf.arrayContains("[]", (Integer)null));
        assertEquals(false, jsonUdf.arrayContains("[]", (Long)null));
        assertEquals(false, jsonUdf.arrayContains("[]", (Double)null));
        assertEquals(false, jsonUdf.arrayContains("[]", (Boolean)null));
        assertEquals(false, jsonUdf.arrayContains("[]", (String)null));
        assertEquals(false, jsonUdf.arrayContains("[]", 1.0));
        assertEquals(false, jsonUdf.arrayContains("[]", 100));
        assertEquals(false, jsonUdf.arrayContains("[]", "abc"));
        assertEquals(false, jsonUdf.arrayContains("[]", ""));
    }

    @Test
    public void shouldNotFindValuesInNullArray() {
        assertEquals(false, jsonUdf.arrayContains("[null]", (Integer)null));
        assertEquals(false, jsonUdf.arrayContains("[null]", (Long)null));
        assertEquals(false, jsonUdf.arrayContains("[null]", (Double)null));
        assertEquals(false, jsonUdf.arrayContains("[null]", (Boolean)null));
        assertEquals(false, jsonUdf.arrayContains("[null]", (String)null));
        assertEquals(false, jsonUdf.arrayContains("[null]", "null"));
        assertEquals(false, jsonUdf.arrayContains("[null]", true));
        assertEquals(false, jsonUdf.arrayContains("[null]", false));
        assertEquals(false, jsonUdf.arrayContains("[null]", 1.0));
        assertEquals(false, jsonUdf.arrayContains("[null]", 100));
        assertEquals(false, jsonUdf.arrayContains("[null]", "abc"));
        assertEquals(false, jsonUdf.arrayContains("[null]", ""));
    }

    @Test
    public void shouldFindIntegersInJsonArray() {
        final String json = "[2147483647, {\"ab\":null }, -2147483648, 1, 2, 3, null, [4], 4]";
        assertEquals(true, jsonUdf.arrayContains(json, 2147483647));
        assertEquals(true, jsonUdf.arrayContains(json, -2147483648));
        assertEquals(true, jsonUdf.arrayContains(json, 1));
        assertEquals(true, jsonUdf.arrayContains(json, 2));
        assertEquals(true, jsonUdf.arrayContains(json, 3));
        assertEquals(false, jsonUdf.arrayContains("5", 5));
        assertEquals(false, jsonUdf.arrayContains(json, 5));
    }

    @Test
    public void shouldFindLongsInJsonArray() {
        assertEquals(true, jsonUdf.arrayContains("[1]", 1L));
        assertEquals(true, jsonUdf.arrayContains("[1111111111111111]", 1111111111111111L));
        assertEquals(true, jsonUdf.arrayContains("[[222222222222222], 33333]", 33333L));
        assertEquals(true, jsonUdf.arrayContains("[{}, \"abc\", null, 1]", 1L));
        assertEquals(false, jsonUdf.arrayContains("[[222222222222222], 33333]", 222222222222222L));
        assertEquals(false, jsonUdf.arrayContains("[{}, \"abc\", null, [1]]", 1L));
        assertEquals(false, jsonUdf.arrayContains("[{}, \"abc\", null, {\"1\":1}]", 1L));
    }

    @Test
    public void shouldFindDoublesInJsonArray() {
        assertEquals(true, jsonUdf.arrayContains("[-1.0, 2.0, 3.0]", 2.0));
        assertEquals(true, jsonUdf.arrayContains("[1.0, -2.0, 3.0]", -2.0));
        assertEquals(true, jsonUdf.arrayContains("[1.0, 2.0, 1.6E3]", 1600.0));
        assertEquals(true, jsonUdf.arrayContains("[1.0, 2.0, -1.6E3]", -1600.0));
        assertEquals(true, jsonUdf.arrayContains("[{}, \"abc\", null, -2.0]", -2.0));
        assertEquals(false, jsonUdf.arrayContains("[[2.0], 3.0]", 2.0));
    }

    @Test
    public void shouldFindStringsInJsonArray() {
        assertEquals(true, jsonUdf.arrayContains("[\"abc\"]", "abc"));
        assertEquals(true, jsonUdf.arrayContains("[\"cbda\", \"abc\"]", "abc"));
        assertEquals(true, jsonUdf.arrayContains("[{}, \"abc\", null, 1]", "abc"));
        assertEquals(true, jsonUdf.arrayContains("[\"\"]", ""));
        assertEquals(false, jsonUdf.arrayContains("[\"\"]", (Integer)null));
        assertEquals(false, jsonUdf.arrayContains("[\"\"]", (Long)null));
        assertEquals(false, jsonUdf.arrayContains("[\"\"]", (Double)null));
        assertEquals(false, jsonUdf.arrayContains("[\"\"]", (Boolean)null));
        assertEquals(false, jsonUdf.arrayContains("[\"\"]", (String)null));

        assertEquals(false, jsonUdf.arrayContains("[1,2,3]", "1"));
        assertEquals(false, jsonUdf.arrayContains("[null]", ""));
        assertEquals(false, jsonUdf.arrayContains("[\"abc\", \"dba\"]", "abd"));
    }

    @Test
    public void shouldFindBooleansInJsonArray() {
        assertEquals(true, jsonUdf.arrayContains("[false, false, true, false]", true));
        assertEquals(true, jsonUdf.arrayContains("[true, true, false]", false));
        assertEquals(false, jsonUdf.arrayContains("[true, true]", false));
        assertEquals(false, jsonUdf.arrayContains("[false, false]", true));
    }

    @Test
    public void shouldReturnFalseOnEmptyList() {
        assertEquals(false, jsonUdf.arrayContains(Collections.emptyList(), true));
        assertEquals(false, jsonUdf.arrayContains(Collections.emptyList(), false));
        assertEquals(false, jsonUdf.arrayContains(Collections.emptyList(), (Integer)null));
        assertEquals(false, jsonUdf.arrayContains(Collections.emptyList(), (Long)null));
        assertEquals(false, jsonUdf.arrayContains(Collections.emptyList(), (Double)null));
        assertEquals(false, jsonUdf.arrayContains(Collections.emptyList(), (String)null));
        assertEquals(false, jsonUdf.arrayContains(Collections.emptyList(), (Boolean)null));
        assertEquals(false, jsonUdf.arrayContains(Collections.emptyList(), 1.0));
        assertEquals(false, jsonUdf.arrayContains(Collections.emptyList(), 100));
        assertEquals(false, jsonUdf.arrayContains(Collections.emptyList(), "abc"));
        assertEquals(false, jsonUdf.arrayContains(Collections.emptyList(), ""));
    }

    @Test
    public void shouldNotFindValuesInNullListElements() {
        assertEquals(false, jsonUdf.arrayContains(Collections.singletonList(null), (Integer)null));
        assertEquals(false, jsonUdf.arrayContains(Collections.singletonList(null), (Long)null));
        assertEquals(false, jsonUdf.arrayContains(Collections.singletonList(null), (Double)null));
        assertEquals(false, jsonUdf.arrayContains(Collections.singletonList(null), (String)null));
        assertEquals(false, jsonUdf.arrayContains(Collections.singletonList(null), (Boolean)null));
        assertEquals(false, jsonUdf.arrayContains(Collections.singletonList(null), "null"));
        assertEquals(false, jsonUdf.arrayContains(Collections.singletonList(null), true));
        assertEquals(false, jsonUdf.arrayContains(Collections.singletonList(null), false));
        assertEquals(false, jsonUdf.arrayContains(Collections.singletonList(null), 1.0));
        assertEquals(false, jsonUdf.arrayContains(Collections.singletonList(null), 100));
        assertEquals(false, jsonUdf.arrayContains(Collections.singletonList(null), "abc"));
        assertEquals(false, jsonUdf.arrayContains(Collections.singletonList(null), ""));
    }

    @Test
    public void shouldFindStringInList() {
        assertEquals(true, jsonUdf.arrayContains(Arrays.asList("abc", "bd", "DC"), "DC"));
        assertEquals(false, jsonUdf.arrayContains(Arrays.asList("abc", "bd", "DC"), "dc"));
        assertEquals(false, jsonUdf.arrayContains(Arrays.asList("abc", "bd", "1"), 1));
    }

    @Test
    public void shouldFindIntegersInList() {
        assertEquals(true, jsonUdf.arrayContains(Arrays.asList(1, 2, 3), 2));
        assertEquals(false, jsonUdf.arrayContains(Arrays.asList(1, 2, 3), 0));
        assertEquals(false, jsonUdf.arrayContains(Arrays.asList(1, 2, 3), "1"));
        assertEquals(false, jsonUdf.arrayContains(Arrays.asList(1, 2, 3), "aa"));
    }

    @Test
    public void shouldFindLongInList() {
        assertEquals(true, jsonUdf.arrayContains(Arrays.asList(1L, 2L, 3L), 2L));
        assertEquals(false, jsonUdf.arrayContains(Arrays.asList(1L, 2L, 3L), 0L));
        assertEquals(false, jsonUdf.arrayContains(Arrays.asList(1L, 2L, 3L), "1"));
        assertEquals(false, jsonUdf.arrayContains(Arrays.asList(1L, 2L, 3L), "aaa"));
    }

    @Test
    public void shouldFindDoublesInList() {
        assertEquals(true, jsonUdf.arrayContains(Arrays.asList(1.0, 2.0, 3.0), 2.0));
        assertEquals(false, jsonUdf.arrayContains(Arrays.asList(1.0, 2.0, 3.0), 4.0));
        assertEquals(false, jsonUdf.arrayContains(Arrays.asList(1.0, 2.0, 3.0), "1"));
        assertEquals(false, jsonUdf.arrayContains(Arrays.asList(1.0, 2.0, 3.0), "aaa"));
    }
}