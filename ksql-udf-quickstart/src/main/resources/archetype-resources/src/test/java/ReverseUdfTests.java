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

package ${package};

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Example class that demonstrates how to unit test UDFs.
 */
public class ReverseUdfTests {

  @ParameterizedTest(name = "reverse({0})= {1}")
  @CsvSource({
    "hello, olleh",
    "world, dlrow",
  })
  void reverseString(final String source, final String expectedResult) {
    final ReverseUdf reverse = new ReverseUdf();
    final String actualResult = reverse.reverseString(source);
    assertEquals(expectedResult, actualResult, source + " reversed should equal " + expectedResult);
  }
}