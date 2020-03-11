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

package io.confluent.ksql.api.server.protocol;

/**
 * Handles errors during deserialization of JSON into POJOs using PojoCodec. The Jackson exceptions
 * are grungy and require parsing to extract useful information so this sanitises it a bit.
 */
public interface PojoDeserializerErrorHandler {

  /**
   * Called when a POJO fails to deserialise because the JSON contains a missing mandatory param
   *
   * @param paramName the name of the param
   */
  void onMissingParam(String paramName);

  /**
   * Called when a POJO fails to deserialise because the JSON contains an extra (unknown) param
   *
   * @param paramName the name of the param
   */
  void onExtraParam(String paramName);

  /**
   * Called when a POJO fails to deserialise because the JSON is not well formed
   */
  void onInvalidJson();
}

