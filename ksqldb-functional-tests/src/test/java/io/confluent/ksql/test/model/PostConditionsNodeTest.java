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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.test.model.PostConditionsNode.PostTopicNode;
import io.confluent.ksql.test.model.PostConditionsNode.PostTopicsNode;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.Test;

public class PostConditionsNodeTest {

  private static final KeyFormat KEY_FORMAT = KeyFormat.nonWindowed(FormatInfo.of("AVRO"));
  private static final ValueFormat VALUE_FORMAT = ValueFormat.of(FormatInfo.of("JSON"));
  private static final OptionalInt PARTITION_COUNT = OptionalInt.of(14);

  @Test
  public void shouldRoundTrip() {
    // Given:
    final PostTopicsNode topics = new PostTopicsNode(
        Optional.of(".*repartition"),
        Optional.of(ImmutableList.of(
            new PostTopicNode("t1", KEY_FORMAT, VALUE_FORMAT, PARTITION_COUNT)
        ))
    );

    final PostConditionsNode postConditions = new PostConditionsNode(
        ImmutableList.of(SourceNodeTest.INSTANCE),
        Optional.of(topics)
    );

    // Then:
    ModelTester.assertRoundTrip(postConditions);
  }
}