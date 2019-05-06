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

package io.confluent.ksql.function.udf.caseexpression;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

@SuppressFBWarnings("NP_BOOLEAN_RETURN_NULL")
public class SearchedCaseFunctionTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldWorkForBooleanValues() {
    // Given:
    final List<SearchedCaseFunction.LazyWhenClause<Boolean>> lazyWhenClauses = ImmutableList.of(
        SearchedCaseFunction.whenClause(() -> false, () -> Boolean.TRUE),
        SearchedCaseFunction.whenClause(() -> false, () -> Boolean.FALSE),
        SearchedCaseFunction.whenClause(() -> true, () -> Boolean.TRUE)
    );

    // When:
    final Boolean result = SearchedCaseFunction.searchedCaseFunction(
        lazyWhenClauses,
        () -> null
    );

    // Then:
    assertThat(result, equalTo(Boolean.TRUE));
  }

  @Test
  public void shouldWorkForIntegerValues() {
    // Given:
    final List<SearchedCaseFunction.LazyWhenClause<Integer>> lazyWhenClauses = ImmutableList.of(
        SearchedCaseFunction.whenClause(() -> false, () -> 1),
        SearchedCaseFunction.whenClause(() -> false, () -> 2),
        SearchedCaseFunction.whenClause(() -> true, () -> 3),
        SearchedCaseFunction.whenClause(() -> true, () -> 4)
    );

    // When:
    final Integer result = SearchedCaseFunction.searchedCaseFunction(
        lazyWhenClauses,
        () -> null
    );

    // Then:
    assertThat(result, equalTo(3));
  }

  @Test
  public void shouldWorkForBigIntValues() {
    // Given:
    final List<SearchedCaseFunction.LazyWhenClause<Long>> lazyWhenClauses = ImmutableList.of(
        SearchedCaseFunction.whenClause(() -> false, () -> 1L),
        SearchedCaseFunction.whenClause(() -> false, () -> 2L),
        SearchedCaseFunction.whenClause(() -> false, () -> 3L),
        SearchedCaseFunction.whenClause(() -> true, () -> 4L)
    );

    // When:
    final Long result = SearchedCaseFunction.searchedCaseFunction(
        lazyWhenClauses,
        () -> null
    );

    // Then:
    assertThat(result, equalTo(4L));
  }

  @Test
  public void shouldWorkForDoubleValues() {
    // Given:
    final List<SearchedCaseFunction.LazyWhenClause<Double>> lazyWhenClauses = ImmutableList.of(
        SearchedCaseFunction.whenClause(() -> false, () -> 1.0),
        SearchedCaseFunction.whenClause(() -> false, () -> 2.0),
        SearchedCaseFunction.whenClause(() -> false, () -> 3.0),
        SearchedCaseFunction.whenClause(() -> true, () -> 4.0)
    );

    // When:
    final Double result = SearchedCaseFunction.searchedCaseFunction(
        lazyWhenClauses,
        () -> null
    );

    // Then:
    assertThat(result, equalTo(4.0));
  }

  @Test
  public void shouldWorkForStringValues() {
    // Given:
    final List<SearchedCaseFunction.LazyWhenClause<String>> lazyWhenClauses = ImmutableList.of(
        SearchedCaseFunction.whenClause(() -> false, () -> "foo"),
        SearchedCaseFunction.whenClause(() -> false, () -> "bar"),
        SearchedCaseFunction.whenClause(() -> false, () -> "tab"),
        SearchedCaseFunction.whenClause(() -> true, () -> "ksql")
    );

    // When:
    final String result = SearchedCaseFunction.searchedCaseFunction(
        lazyWhenClauses,
        () -> null
    );

    // Then:
    assertThat(result, equalTo("ksql"));
  }

  @Test
  public void shouldWorkForArrayValues() {
    // Given:
    final List<SearchedCaseFunction.LazyWhenClause<List<String>>> lazyWhenClauses = ImmutableList.of(
        SearchedCaseFunction.whenClause(() -> false, () -> ImmutableList.of("foo", "bar")),
        SearchedCaseFunction.whenClause(() -> true, () -> ImmutableList.of("tab", "ksql"))
    );

    // When:
    final List<String> result = SearchedCaseFunction.searchedCaseFunction(
        lazyWhenClauses,
        () -> null
    );

    // Then:
    assertThat(result, equalTo(ImmutableList.of("tab", "ksql")));
  }

  @Test
  public void shouldWorkForMapValues() {
    // Given:
    final List<SearchedCaseFunction.LazyWhenClause<Map<String, Double>>> lazyWhenClauses = ImmutableList.of(
        SearchedCaseFunction.whenClause(() -> false, () -> ImmutableMap.of("foo", 1.0)),
        SearchedCaseFunction.whenClause(() -> true, () -> ImmutableMap.of("tab", 2.0))
    );

    // When:
    final Map<String, Double> result = SearchedCaseFunction.searchedCaseFunction(
        lazyWhenClauses,
        () -> null
    );

    // Then:
    assertThat(result, equalTo(ImmutableMap.of("tab", 2.0)));
  }

  @Test
  public void shouldWorkForStructValues() {
    // Given:
    final List<SearchedCaseFunction.LazyWhenClause<Map<String, Object>>> lazyWhenClauses = ImmutableList.of(
        SearchedCaseFunction.whenClause(() -> false, () -> ImmutableMap.of("foo", 1.0)),
        SearchedCaseFunction.whenClause(() -> true, () -> ImmutableMap.of("tab", "ksql"))
    );

    // When:
    final Map<String, Object> result = SearchedCaseFunction.searchedCaseFunction(
        lazyWhenClauses,
        () -> null
    );

    // Then:
    assertThat(result, equalTo(ImmutableMap.of("tab", "ksql")));
  }

  @Test
  public void shouldReturnDefaultIfNoMatch() {
    // Given:
    final List<SearchedCaseFunction.LazyWhenClause<Integer>> lazyWhenClauses = ImmutableList.of(
        SearchedCaseFunction.whenClause(() -> false, () -> 1),
        SearchedCaseFunction.whenClause(() -> false, () -> 2),
        SearchedCaseFunction.whenClause(() -> false, () -> 3),
        SearchedCaseFunction.whenClause(() -> false, () -> 4)
    );

    // When:
    final Integer result = SearchedCaseFunction.searchedCaseFunction(
        lazyWhenClauses,
        () -> 10
    );

    // Then:
    assertThat(result, equalTo(10));
  }
  
}
