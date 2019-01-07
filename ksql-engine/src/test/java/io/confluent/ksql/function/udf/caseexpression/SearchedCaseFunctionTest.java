/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.caseexpression;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SearchedCaseFunctionTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldWorkForBooleanValues() {
    // Given:
    final List<Boolean> whenList = ImmutableList.of(false, false, true);
    final List<Boolean> thenList = ImmutableList.of(Boolean.TRUE, Boolean.FALSE, Boolean.TRUE);


    // When:
    final Boolean result = SearchedCaseFunction.searchedCaseFunction(
        whenList,
        thenList,
        null
    );

    // Then:
    assertThat(result, equalTo(Boolean.TRUE));
  }

  @Test
  public void shouldWorkForIntegerValues() {
    // Given:
    final List<Boolean> whenList = ImmutableList.of(false, false, true, true);
    final List<Integer> thenList = ImmutableList.of(1, 2, 3, 4);


    // When:
    final Integer result = SearchedCaseFunction.searchedCaseFunction(
        whenList,
        thenList,
        null
    );

    // Then:
    assertThat(result, equalTo(3));
  }

  @Test
  public void shouldWorkForBigIntValues() {
    // Given:
    final List<Boolean> whenList = ImmutableList.of(false, false, false, true);
    final List<Long> thenList = ImmutableList.of(1L, 2L, 3L, 4L);


    // When:
    final Long result = SearchedCaseFunction.searchedCaseFunction(
        whenList,
        thenList,
        null
    );

    // Then:
    assertThat(result, equalTo(4L));
  }

  @Test
  public void shouldWorkForDoubleValues() {
    // Given:
    final List<Boolean> whenList = ImmutableList.of(false, false, false, true);
    final List<Double> thenList = ImmutableList.of(1.0, 2.0, 3.0, 4.0);


    // When:
    final Double result = SearchedCaseFunction.searchedCaseFunction(
        whenList,
        thenList,
        null
    );

    // Then:
    assertThat(result, equalTo(4.0));
  }

  @Test
  public void shouldWorkForStringValues() {
    // Given:
    final List<Boolean> whenList = ImmutableList.of(false, false, false, true);
    final List<String> thenList = ImmutableList.of("foo", "bar", "tab", "ksql");


    // When:
    final String result = SearchedCaseFunction.searchedCaseFunction(
        whenList,
        thenList,
        null
    );

    // Then:
    assertThat(result, equalTo("ksql"));
  }

  @Test
  public void shouldWorkForArrayValues() {
    // Given:
    final List<Boolean> whenList = ImmutableList.of(false, true);
    final List<List<String>> thenList = ImmutableList.of(
        ImmutableList.of("foo", "bar"), ImmutableList.of("tab", "ksql"));


    // When:
    final List<String> result = SearchedCaseFunction.searchedCaseFunction(
        whenList,
        thenList,
        null
    );

    // Then:
    assertThat(result, equalTo(ImmutableList.of("tab", "ksql")));
  }

  @Test
  public void shouldWorkForMapValues() {
    // Given:
    final List<Boolean> whenList = ImmutableList.of(false, true);
    final List<Map<String, Double>> thenList = ImmutableList.of(
        ImmutableMap.of("foo", 1.0), ImmutableMap.of("tab", 2.0));


    // When:
    final Map<String, Double> result =
        SearchedCaseFunction.searchedCaseFunction(
            whenList,
            thenList,
            null
        );

    // Then:
    assertThat(result, equalTo(ImmutableMap.of("tab", 2.0)));
  }

  @Test
  public void shouldWorkForStructValues() {
    // Given:
    final List<Boolean> whenList = ImmutableList.of(false, true);
    final List<Map<String, Object>> thenList = ImmutableList.of(
        ImmutableMap.of("foo", 1.0), ImmutableMap.of("tab", "ksql"));


    // When:
    final Map<String, Object> result =
        SearchedCaseFunction.searchedCaseFunction(
            whenList,
            thenList,
            null
        );

    // Then:
    assertThat(result, equalTo(ImmutableMap.of("tab", "ksql")));
  }

  @Test
  public void shouldReturnDefaultIfNoMatch() {
    // Given:
    final List<Boolean> whenList = ImmutableList.of(false, false, false, false);
    final List<Integer> thenList = ImmutableList.of(1, 2, 3, 4);


    // When:
    final Integer result = SearchedCaseFunction.searchedCaseFunction(
        whenList,
        thenList,
        10
    );

    // Then:
    assertThat(result, equalTo(10));
  }

  @Test
  public void shouldFailIfWhenAndThenDontHaveSameSize() {
    // Given:
    final List<Boolean> whenList = ImmutableList.of(false, false, true, true);
    final List<Integer> thenList = ImmutableList.of(1, 2, 3);
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Invalid arguments. "
        + "When list and Then list should have the same size. "
        + "When list size is 4, thenList size is 3");


    // When:
    SearchedCaseFunction.searchedCaseFunction(
        whenList,
        thenList,
        null
    );

  }

}
