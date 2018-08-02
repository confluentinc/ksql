/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.util.timestamp;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.GenericRow;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

public class StringTimestampExtractorTest {

  private final String format = "yyyy-MMM-dd";

  @SuppressWarnings("unchecked")
  @Test
  public void shouldExtractTimestampFromStringWithFormat() throws ParseException {
    final StringTimestampExtractor timestampExtractor = new StringTimestampExtractor(format, 0);

    final String stringTime = "2010-Jan-11";
    final long expectedTime = new SimpleDateFormat(format).parse(stringTime).getTime();
    final long actualTime = timestampExtractor.extract(new ConsumerRecord("topic",
        1,
        1,
        null,
        new GenericRow(Collections.singletonList(stringTime))), 1);
    assertThat(actualTime, equalTo(expectedTime));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfColumnIndexIsNegative() {
    new StringTimestampExtractor(format, -1);
  }


  @SuppressWarnings("unchecked")
  @Test(expected = NullPointerException.class)
  public void shouldThrowOnNullFormat() {
    new StringTimestampExtractor(null, -1);
  }
}