/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.query;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class RecordTooLargeClassifierTest {

  @Test
  public void shouldClassifyRecordTooLargeExceptionAsUserError() {
    // Given:
    final Exception e = new StreamsException(
        "Error encountered trying to send record to topic foo",
        new KafkaException(
            "Cannot execute transactional method because we are in an error state",
            new RecordTooLargeException(
                "The message is 1084728 bytes when serialized which is larger than 1048576, which"
                    + " is the value of the max.request.size configuration.")
        )
    );

    // When:
    final QueryError.Type type = new RecordTooLargeClassifier("").classify(e);

    // Then:
    assertThat(type, is(QueryError.Type.USER));
  }

}