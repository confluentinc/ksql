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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Locale;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;

public enum ConnectorType {
  SOURCE,
  SINK,
  UNKNOWN;

  ConnectorType() {
  }

  public static ConnectorType from(final Class<? extends Connector> clazz) {
    if (SinkConnector.class.isAssignableFrom(clazz)) {
      return SINK;
    } else {
      return SourceConnector.class.isAssignableFrom(clazz) ? SOURCE : UNKNOWN;
    }
  }

  @JsonValue
  public String toString() {
    return super.toString().toLowerCase(Locale.ROOT);
  }

  @JsonCreator
  public static ConnectorType forValue(final String value) {
    return valueOf(value.toUpperCase(Locale.ROOT));
  }
}