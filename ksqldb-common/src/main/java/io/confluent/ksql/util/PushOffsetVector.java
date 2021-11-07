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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;

/**
 * OffsetVector for push query continuation tokens.
 */
public class PushOffsetVector implements OffsetVector {

  private final List<Long> offsets;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  @JsonCreator
  public PushOffsetVector(final @JsonProperty(value = "o") List<Long> offsets) {
    this.offsets = offsets;
  }

  @Override
  public void merge(final OffsetVector other) {
    throw new UnsupportedOperationException("Unsupported");
  }

  @Override
  public boolean lessThanOrEqualTo(final OffsetVector other) {
    throw new UnsupportedOperationException("Unsupported");
  }

  @JsonIgnore
  @Override
  public List<Long> getDenseRepresentation() {
    return getOffsets();
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  @JsonProperty("o")
  public List<Long> getOffsets() {
    return ImmutableList.copyOf(offsets);
  }

  @JsonIgnore
  @Override
  public boolean dominates(final OffsetVector other) {
    throw new UnsupportedOperationException("Unsupported");
  }

  @Override
  public void update(final String topic, final int partition, final long offset) {
    throw new UnsupportedOperationException("Unsupported");
  }

  @Override
  public String serialize() {
    throw new UnsupportedOperationException("Unsupported");
  }
}
