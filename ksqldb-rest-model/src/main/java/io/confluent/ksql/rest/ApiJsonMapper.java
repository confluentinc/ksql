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

package io.confluent.ksql.rest;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.confluent.ksql.json.KsqlTypesSerializationModule;
import io.confluent.ksql.json.StructSerializationModule;
import io.confluent.ksql.util.KsqlConstants;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Mapper used by the Rest Api.
 */
public enum ApiJsonMapper {

  INSTANCE;

  private final ObjectMapper mapper = new ObjectMapper()
      .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
      .registerModule(new Jdk8Module())
      .registerModule(new StructSerializationModule())
      .registerModule(new KsqlTypesSerializationModule())
      .registerModule(new GuavaModule())
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .disable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
      .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
      .setDateFormat(new SimpleDateFormat(KsqlConstants.DATE_TIME_PATTERN))
      .setTimeZone(TimeZone.getTimeZone("Z"))
      .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

  public ObjectMapper get() {
    return mapper;
  }
}