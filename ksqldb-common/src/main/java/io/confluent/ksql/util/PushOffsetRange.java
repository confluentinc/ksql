/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;

//@JsonInclude(JsonInclude.Include.NON_ABSENT)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PushOffsetRange {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    OBJECT_MAPPER.registerModule(new Jdk8Module());
  }

  private final int version;
  // If set, the start of the offset range associated with this batch of data.
  private final Optional<PushOffsetVector> startOffsets;
  // The offsets associated with this row/batch of rows, or the end of the offset range if start
  // offsets is set.
  private final PushOffsetVector endOffsets;

  @JsonCreator
  public PushOffsetRange(
      final @JsonProperty(value = "s") Optional<PushOffsetVector> startOffsets,
      final @JsonProperty(value = "e", required = true) PushOffsetVector endOffsets,
      final @JsonProperty(value = "v", required = true) int version
  ) {
    this.startOffsets = startOffsets;
    this.endOffsets = endOffsets;
    this.version = version;
  }

  public PushOffsetRange(
      final Optional<PushOffsetVector> startOffsets,
      final PushOffsetVector endOffsets
  ) {
    this.startOffsets = startOffsets;
    this.endOffsets = endOffsets;
    this.version = 0;
  }

  /**
   * If this represents a range of offsets, the starting offsets of the range.
   */
  @JsonProperty("s")
  public Optional<PushOffsetVector> getStartOffsets() {
    return startOffsets.map(o -> o);
  }

  /**
   * The offsets associated with this set of rows, or the end of a range, if start is also set.
   */
  @JsonProperty("e")
  public PushOffsetVector getEndOffsets() {
    return endOffsets;
  }

  @JsonProperty("v")
  public int getVersion() {
    return version;
  }

  public String serialize() {
    try {
      final byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(this);
      return Base64.getEncoder().encodeToString(bytes);
    } catch (Exception e) {
      throw new KsqlException("Couldn't encode push offset range token", e);
    }
  }

  public static PushOffsetRange deserialize(final String token) {
    try {
      final byte[] bytes = Base64.getDecoder().decode(token);
      return OBJECT_MAPPER.readValue(bytes, PushOffsetRange.class);
    } catch (Exception e) {
      throw new KsqlException("Couldn't decode push offset range token", e);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PushOffsetRange that = (PushOffsetRange) o;
    return Objects.equals(version, that.version)
        && Objects.equals(startOffsets, that.startOffsets)
        && Objects.equals(endOffsets, that.endOffsets);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, startOffsets, endOffsets);
  }

  @JsonIgnore
  @Override
  public String toString() {
    return "PushOffsetRange{"
        + "version=" + version
        + ", startOffsets=" + startOffsets
        + ", endOffsets=" + endOffsets
        + '}';
  }
}
