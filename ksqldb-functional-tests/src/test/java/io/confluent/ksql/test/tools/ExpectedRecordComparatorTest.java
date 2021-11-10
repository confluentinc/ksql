/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.test.tools;

import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.junit.Test;

public class ExpectedRecordComparatorTest {

  @Test
  public void shouldMatchActualBigDecimalWithZeroScaleToIntNode() {
    assertMatch(new BigDecimal("10"), new IntNode(10));
  }

  @Test
  public void shouldMatchActualBigDecimalWithNegativeScaleToIntNode() {
    assertMatch(new BigDecimal("10").setScale(-1, RoundingMode.UNNECESSARY), new IntNode(10));
  }

  @Test
  public void shouldNotMatchActualBigDecimalWithNonZeroScaleToIntNode() {
    assertNoMatch(new BigDecimal("10.0"), new IntNode(10));
  }

  @Test
  public void shouldMatchActualBigDecimalWithZeroScaleToLongNode() {
    assertMatch(new BigDecimal("10"), new LongNode(10L));
  }

  @Test
  public void shouldNotMatchActualBigDecimalWithNonZeroScaleToLongNode() {
    assertNoMatch(new BigDecimal("10.0"), new LongNode(10));
  }

  @Test
  public void shouldMatchActualBigDecimalWithNegativeScaleToLongNode() {
    assertMatch(new BigDecimal("10").setScale(-1, RoundingMode.UNNECESSARY), new LongNode(10));
  }

  @Test
  public void shouldNotMatchActualBigDecimalWithDoubleNode() {
    assertNoMatch(new BigDecimal("10.1"), new DoubleNode(10.1));
  }

  @Test
  public void shouldMatchActualBigDecimalWithMatchingDecimalNode() {
    assertMatch(new BigDecimal("10.01"), new DecimalNode(new BigDecimal("10.01")));
  }

  @Test
  public void shouldNotMatchActualBigDecimalWithDecimalNodeWithHigherScale() {
    assertNoMatch(new BigDecimal("10.01"), new DecimalNode(new BigDecimal("10.010")));
  }

  @Test
  public void shouldNotMatchActualBigDecimalWithDecimalNodeWithLowerScale() {
    assertNoMatch(new BigDecimal("10.010"), new DecimalNode(new BigDecimal("10.01")));
  }

  private static void assertMatch(final Object actual, final JsonNode expected) {
    assertThat(
        "should match"
            + System.lineSeparator()
            + "Expected: " + expected
            + System.lineSeparator()
            + "Actual: " + actual,
        !ExpectedRecordComparator.matches(actual, expected).isPresent()
    );
  }

  private static void assertNoMatch(final Object actual, final JsonNode expected) {
    assertThat(
        "should not match"
            + System.lineSeparator()
            + "Expected: " + expected
            + System.lineSeparator()
            + "Actual: " + actual,
        ExpectedRecordComparator.matches(actual, expected).isPresent()
    );
  }
}