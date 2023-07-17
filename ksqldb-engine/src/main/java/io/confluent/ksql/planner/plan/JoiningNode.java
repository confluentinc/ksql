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

package io.confluent.ksql.planner.plan;

import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.ValueFormat;
import java.util.Optional;

/**
 * A node that collaborates in joins.
 */
public interface JoiningNode {

  /**
   * Get any <b>preferred</b> key format.
   *
   * <p>Any side that is already being repartitioned has no preferred key format.
   *
   * @return the preferred key format, if there is one.
   */
  Optional<KeyFormat> getPreferredKeyFormat();

  /**
   * Set the key format.
   *
   * @param format the key format.
   */
  void setKeyFormat(KeyFormat format);

  static ValueFormat getValueFormatForSource(final PlanNode sourceNode) {
    return sourceNode.getLeftmostSourceNode()
        .getDataSource()
        .getKsqlTopic()
        .getValueFormat();
  }

}
