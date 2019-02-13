/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.math.DoubleMath;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("metric")
public class Metric {

  private final String name;
  private final double value;
  private final long timestamp;
  private final boolean isError;

  @JsonCreator
  public Metric(
      @JsonProperty("name") final String name,
      @JsonProperty("value") final double value,
      @JsonProperty("timestamp") final long timestamp,
      @JsonProperty("isError") final boolean isError) {
    this.name = name;
    this.value = value;
    this.timestamp = timestamp;
    this.isError = isError;
  }

  public String getName() {
    return name;
  }

  public double getValue() {
    return value;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public boolean isError() {
    return isError;
  }

  public String formatted() {
    return (DoubleMath.isMathematicalInteger(value))
        ? String.format("%16s:%10.0f", name, value)
        : String.format("%16s:%10.2f", name, value);
  }

  @Override
  public String toString() {
    return "Metric{" + "name='" + name + '\''
        + ", value=" + value
        + ", timestamp=" + timestamp
        + ", isError=" + isError
        + '}';
  }
}
