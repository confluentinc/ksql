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

import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.confluent.ksql.util.KsqlConfig;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class LongTimestampExtractionPolicyTest {

  private final KsqlConfig config = new KsqlConfig(Collections.singletonMap(
      KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX, 1));

  private final Map<String, Object> properties = new HashMap<>();
  private final LongColumnTimestampExtractionPolicy policy
      = new LongColumnTimestampExtractionPolicy("field");

  @Test
  public void shouldSetTimestampColumnIndexFromConfig() {
    policy.applyTo(config, properties);
    assertThat(properties.get(KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX), equalTo(1));
  }

  @Test
  public void shouldSetTimestampExtractorToLongTimestampExtractor() {
    policy.applyTo(config, properties);
    assertThat(properties.get(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG),
        equalTo(LongTimestampExtractor.class));
  }

}