/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.function.udf.string;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.util.Collections;
import java.util.Map;
import org.junit.Test;

public class SplitToMapTest {
  private static final SplitToMap udf = new SplitToMap();

  @Test
  public void shouldSplitStringByGivenDelimiterChars() {
    Map<String, String> result = udf.splitToMap("foo=apple;bar=cherry", ";", "=");
    assertThat(result, hasEntry("foo", "apple"));
    assertThat(result, hasEntry("bar", "cherry"));
    assertThat(result.size(), equalTo(2));
  }

  @Test
  public void shouldSplitStringGivenMultiCharDelimiters() {
    Map<String, String> result = udf.splitToMap("foo:=apple||bar:=cherry", "||", ":=");
    assertThat(result, hasEntry("foo", "apple"));
    assertThat(result, hasEntry("bar", "cherry"));
    assertThat(result.size(), equalTo(2));
  }

  @Test
  public void shouldSplitStringWithOnlyOneEntry() {
    Map<String, String> result = udf.splitToMap("foo=apple", ";", "=");
    assertThat(result, hasEntry("foo", "apple"));
    assertThat(result.size(), equalTo(1));
  }

  @Test
  public void shouldRetainWhitespacebetweenDelimiters() {
    Map<String, String> result = udf.splitToMap("foo :=\tapple || bar:=cherry", "||", ":=");
    assertThat(result, hasEntry("foo ", "\tapple "));
    assertThat(result, hasEntry(" bar", "cherry"));
    assertThat(result.size(), equalTo(2));
  }

  @Test
  public void shouldDropEmptyEntriesFromSplit() {
    Map<String, String> result = udf.splitToMap("/foo:=apple//bar:=cherry/", "/", ":=");
    assertThat(result, hasEntry("foo", "apple"));
    assertThat(result, hasEntry("bar", "cherry"));
    assertThat(result.size(), equalTo(2));
  }

  @Test
  public void shouldDropEntriesWithoutKeyAndValue() {
    Map<String, String> result = udf.splitToMap("foo:=apple/cherry", "/", ":=");
    assertThat(result, hasEntry("foo", "apple"));
    assertThat(result.size(), equalTo(1));
  }

  @Test
  public void shouldReturnEmptyForInputWithoutDelimiters() {
    Map<String, String> result = udf.splitToMap("cherry", "/", ":=");
    assertThat(result, is(Collections.EMPTY_MAP));
  }

  @Test
  public void shouldReturnEmptyForEmptyInput() {
    Map<String, String> result = udf.splitToMap("", "/", ":=");
    assertThat(result, is(Collections.EMPTY_MAP));
  }

  @Test
  public void shouldKeepLatestValueForDuplicateKey() {
    Map<String, String> result = udf.splitToMap("/foo:=apple/foo:=cherry/", "/", ":=");
    assertThat(result, hasEntry("foo", "cherry"));
    assertThat(result.size(), equalTo(1));
  }

  @Test
  public void shouldReturnNullOnNullInputString() {
    Map<String, String> result = udf.splitToMap(null, "/", ":=");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullOnSameDelimiterChars() {
    Map<String, String> result = udf.splitToMap("foo:=apple/bar:=cherry", "/", "/");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullOnEmptyEntryDelimiter() {
    Map<String, String> result = udf.splitToMap("foo:=apple/bar:=cherry", "", ":=");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullOnNullEntryDelimiter() {
    Map<String, String> result = udf.splitToMap("foo:=apple/bar:=cherry", null, ":=");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullOnEmptyValueDelimiter() {
    Map<String, String> result = udf.splitToMap("foo:=apple/bar:=cherry", "/", "");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullOnNullValueDelimiter() {
    Map<String, String> result = udf.splitToMap("foo:=apple/bar:=cherry", "/", null);
    assertThat(result, is(nullValue()));
  }

}
