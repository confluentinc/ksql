/*
 * Copyright 2019 Confluent Inc.
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

public class JsonArrayContainsTest
{
    private final JsonArrayContains jsonUdf = new JsonArrayContains();

    @Test
    public void shouldReturnFalseOnEmptyArray() {
        assertEquals(false, jsonUdf.contains("[]", true));
        assertEquals(false, jsonUdf.contains("[]", false));
        assertEquals(false, jsonUdf.contains("[]", null));
        assertEquals(false, jsonUdf.contains("[]", 1.0));
        assertEquals(false, jsonUdf.contains("[]", 100));
        assertEquals(false, jsonUdf.contains("[]", "abc"));
        assertEquals(false, jsonUdf.contains("[]", ""));
    }

    @Test
    public void shouldNotFindValuesInNullArray() {
        assertEquals(true, jsonUdf.contains("[null]", null));
        assertEquals(false, jsonUdf.contains("[null]", "null"));
        assertEquals(false, jsonUdf.contains("[null]", true));
        assertEquals(false, jsonUdf.contains("[null]", false));
        assertEquals(false, jsonUdf.contains("[null]", 1.0));
        assertEquals(false, jsonUdf.contains("[null]", 100));
        assertEquals(false, jsonUdf.contains("[null]", "abc"));
        assertEquals(false, jsonUdf.contains("[null]", ""));
    }

    @Test
    public void shouldFindIntegersInJsonArray() {
        final String json = "[2147483647, {\"ab\":null }, -2147483648, 1, 2, 3, null, [4], 4]";
        assertEquals(true, jsonUdf.contains(json, 2147483647));
        assertEquals(true, jsonUdf.contains(json, -2147483648));
        assertEquals(true, jsonUdf.contains(json, 1));
        assertEquals(true, jsonUdf.contains(json, 2));
        assertEquals(true, jsonUdf.contains(json, 3));
        assertEquals(false, jsonUdf.contains("5", 5));
        assertEquals(false, jsonUdf.contains(json, 5));
    }

    @Test
    public void shouldFindLongsInJsonArray() {
        assertEquals(true, jsonUdf.contains("[1]", 1L));
        assertEquals(true, jsonUdf.contains("[1111111111111111]", 1111111111111111L));
        assertEquals(true, jsonUdf.contains("[[222222222222222], 33333]", 33333L));
        assertEquals(true, jsonUdf.contains("[{}, \"abc\", null, 1]", 1L));
        assertEquals(false, jsonUdf.contains("[[222222222222222], 33333]", 222222222222222L));
        assertEquals(false, jsonUdf.contains("[{}, \"abc\", null, [1]]", 1L));
        assertEquals(false, jsonUdf.contains("[{}, \"abc\", null, {\"1\":1}]", 1L));
        assertEquals(false, jsonUdf.contains("[1]", 1.0));
    }

    @Test
    public void shouldFindDoublesInJsonArray() {
        assertEquals(true, jsonUdf.contains("[-1.0, 2.0, 3.0]", 2.0));
        assertEquals(true, jsonUdf.contains("[1.0, -2.0, 3.0]", -2.0));
        assertEquals(true, jsonUdf.contains("[1.0, 2.0, 1.6E3]", 1600.0));
        assertEquals(true, jsonUdf.contains("[1.0, 2.0, -1.6E3]", -1600.0));
        assertEquals(true, jsonUdf.contains("[{}, \"abc\", null, -2.0]", -2.0));
        assertEquals(false, jsonUdf.contains("[[2.0], 3.0]", 2.0));
    }

    @Test
    public void shouldFindStringsInJsonArray() {
        assertEquals(true, jsonUdf.contains("[\"abc\"]", "abc"));
        assertEquals(true, jsonUdf.contains("[\"cbda\", \"abc\"]", "abc"));
        assertEquals(true, jsonUdf.contains("[{}, \"abc\", null, 1]", "abc"));
        assertEquals(true, jsonUdf.contains("[\"\"]", ""));
        assertEquals(false, jsonUdf.contains("[\"\"]", null));
        assertEquals(false, jsonUdf.contains("[1,2,3]", "1"));
        assertEquals(false, jsonUdf.contains("[null]", ""));
        assertEquals(false, jsonUdf.contains("[\"abc\", \"dba\"]", "abd"));
    }

    @Test
    public void shouldFindBooleansInJsonArray() {
        assertEquals(true, jsonUdf.contains("[false, false, true, false]", true));
        assertEquals(true, jsonUdf.contains("[true, true, false]", false));
        assertEquals(false, jsonUdf.contains("[true, true]", false));
        assertEquals(false, jsonUdf.contains("[false, false]", true));
    }

    @Test
    public void shouldHandleNullsInJsonArray() {
        assertEquals(false, jsonUdf.contains("[false, false, true, false]", null));
    }
}