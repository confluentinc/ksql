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

package io.confluent.ksql.json;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public class JsonMapperTest {

  private static final ObjectMapper OBJECT_MAPPER = JsonMapper.INSTANCE.mapper;

  @Test
  public void shouldNotAutoCloseTarget() {
    assertThat(OBJECT_MAPPER.isEnabled(JsonGenerator.Feature.AUTO_CLOSE_TARGET), is(false));
  }

  @Test
  public void shouldIgnoreUnknownProperties() {
    assertThat(OBJECT_MAPPER.isEnabled(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES), is(false));
  }
}