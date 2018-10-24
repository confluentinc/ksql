/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.rest.entity;

import static io.confluent.ksql.util.KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Test;

public class KsqlRequestTest {

  @Test
  public void shouldCoerceShortPropertyValues() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of(
        SINK_NUMBER_OF_REPLICAS_PROPERTY, 2
    );

    // When:
    final KsqlRequest request = new KsqlRequest("stmt", properties);

    // Then:
    assertThat(request.getStreamsProperties().get(SINK_NUMBER_OF_REPLICAS_PROPERTY), is((short)2));
  }
}