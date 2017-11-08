/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashMap;
import java.util.Map;

public class KsqlConfigTest {

  @Test
  public void shouldSetInitialValuesCorrectly() {
    Map<String, Object> initialProps = new HashMap<>();
    initialProps.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 10);
    initialProps.put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, (short) 3);
    initialProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 800);
    initialProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 5);

    KsqlConfig ksqlConfig = new KsqlConfig(initialProps);

    assertThat(ksqlConfig.get(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY), equalTo(10));
    assertThat(ksqlConfig.get(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY), equalTo((short) 3));

  }


}
