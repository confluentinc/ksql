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

package io.confluent.ksql.test.model;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.serde.SerdeOption;
import java.util.Collections;
import java.util.Optional;
import org.junit.Test;

public class SourceNodeTest {

  static final SourceNode INSTANCE = new SourceNode(
      "bob",
      "stream",
      Optional.of("ROWKEY INT KEY, NAME STRING"),
      Optional.of(KeyFormatNodeTest.INSTANCE),
      Optional.of(ImmutableSet.of(SerdeOption.UNWRAP_SINGLE_VALUES))
  );
  static final SourceNode INSTANCE_WITHOUT_SERDE_OPTIONS = new SourceNode(
      "bob",
      "stream",
      Optional.of("ROWKEY INT KEY, NAME STRING"),
      Optional.of(KeyFormatNodeTest.INSTANCE),
      Optional.empty()
  );
  static final SourceNode INSTANCE_WITH_EMPTY_SERDE_OPTIONS = new SourceNode(
      "bob",
      "stream",
      Optional.of("ROWKEY INT KEY, NAME STRING"),
      Optional.of(KeyFormatNodeTest.INSTANCE),
      Optional.of(Collections.emptySet())
  );

  @Test
  public void shouldRoundTrip() {
    ModelTester.assertRoundTrip(INSTANCE);
  }

  @Test
  public void shouldRoundTripWithoutSerdeOptions() {
    ModelTester.assertRoundTrip(INSTANCE_WITHOUT_SERDE_OPTIONS);
  }

  @Test
  public void shouldRoundTripWithEmptySerdeOptions() {
    ModelTester.assertRoundTrip(INSTANCE_WITH_EMPTY_SERDE_OPTIONS);
  }
}