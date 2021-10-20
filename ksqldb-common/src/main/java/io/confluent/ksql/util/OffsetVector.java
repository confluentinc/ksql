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

import java.util.List;

/**
 * Interface representing a vector of offsets. This is a common interface for both consistency work
 * as well as push query continuation offsets.
 */
public interface OffsetVector {
  void merge(OffsetVector other);

  /**
   * Returns true if all partitions of this vector have offsets less than or equal to other.
   */
  boolean lessThanOrEqualTo(OffsetVector other);

  /**
   * The dense representation of the offset vector, namely a list of offsets where each index
   * corresponds to the partition.
   * @return The list of offsets
   */
  List<Long> getDenseRepresentation();
}
